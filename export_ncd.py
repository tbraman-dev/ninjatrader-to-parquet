"""Export NinjaTrader 8 .ncd history to parquet without NinjaTrader, then stitch one continuous file per symbol.

    python export_ncd.py                          # every futures root in db/, minute Last/Bid/Ask + tick Last
    python export_ncd.py --symbols ES --kinds minute --no-stitch
    python export_ncd.py --force --csv            # re-export every day; also write continuous csv.gz

Contracts:   the folders of <nt8-dir>/db/minute and db/tick named "<ROOT> MM-YY" (empty folders are ignored).
Per day:     <out>/<kind>/<type>/<CONTRACT_>/<YYYYMMDD>.parquet   (utc_us int64, open/high/low/close float64, volume int64; zstd)
Continuous:  <out>/continuous/<SYM>_1min_<type>.parquet, <SYM>_tick_<type>.parquet  (+ contract, dictionary string)
             --csv also writes the same rows as <SYM>_..._<type>.csv.gz (header line, prices written like NT8's export).
Roll:        --roll volume (default) switches from one contract to the next on the first day the next one trades
             more (daily Last volume of the per-day files); --roll calendar uses a fixed date rule. Cut at 18:00 ET.
Incremental: manifest.json per contract dir keeps each source's size + mtime_ns; unchanged days are skipped.
Session filter: rows outside the instrument's NT8 trading-hours template (daily halt, weekend, holidays, early closes)
             are dropped like NT8's own export does; --raw keeps them. After a template update or a --raw switch, run --force.
Time zone:   .ncd times are NT8's local time; --tz names that zone (default America/New_York). After a change, run --force.
NT8 files are only opened read-only; nothing is written under the NT8 folder.
"""
import argparse
import datetime as dt
import functools
import gzip
import json
import multiprocessing as mp
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

import ncd

FIELDS = ("utc_us", "open", "high", "low", "close", "volume")
DAY_SCHEMA = pa.schema([("utc_us", pa.int64())] + [(k, pa.float64()) for k in FIELDS[1:5]] + [("volume", pa.int64())])
CONT_SCHEMA = DAY_SCHEMA.append(pa.field("contract", pa.dictionary(pa.int32(), pa.string())))
CSV_OPTS = pcsv.WriteOptions(include_header=False, quoting_style="none")
CONTRACT = re.compile(r"(\S+) (\d\d)-(\d\d)")
# Calendar roll rule: --roll calendar, and the fallback when a pair of contracts has no volume to compare.
# Roots not listed use the default rule.
ROLL_26TH_OF_PRIOR_MONTH = {"GC", "SI", "HG", "PL", "PA", "MGC", "SIL"}  # metals; default: 8 days before 3rd Friday


def roll_date(root, y, m):
    """Day the continuous file switches to contract (root, y, m); window edges are UTC midnight of this day."""
    if root in ROLL_26TH_OF_PRIOR_MONTH:
        return dt.date(y, m - 1, 26) if m > 1 else dt.date(y - 1, 12, 26)
    d = dt.date(y, m, 15)
    return d + dt.timedelta(days=(4 - d.weekday()) % 7 - 8)  # 3rd Friday - 8 days


def discover(db):
    """{root: [(contract, roll date), ...] ordered by expiry} from the contract folders under db/minute and db/tick."""
    found = {}
    for kind in ("minute", "tick"):
        d = os.path.join(db, kind)
        for name in os.listdir(d) if os.path.isdir(d) else []:
            m = CONTRACT.fullmatch(name)
            if m and name not in found.get(m[1], {}):
                with os.scandir(os.path.join(d, name)) as it:
                    if any(e.name.endswith(".ncd") for e in it):
                        found.setdefault(m[1], {})[name] = roll_date(m[1], 2000 + int(m[3]), int(m[2]))
    return {r: sorted(c.items(), key=lambda x: x[1]) for r, c in sorted(found.items())}


def pick_rolls(cons, vol, read=lambda v: v):
    """Volume roll: [(contract, roll date)] like discover(), each date replaced by the first day the next contract's
    volume beats this one's. vol: {contract: {date: volume, or what read() turns into it}}; read runs only on days
    both contracts have. Never rolls back. A day this contract has no data but has data later is a gap, skipped;
    after its last day it counts as 0. No data for either contract of a pair: keep the calendar date."""
    out, start = [], None
    for i, (a, cal) in enumerate(cons):
        va, vb = vol.get(a, {}), vol.get(cons[i + 1][0], {}) if i + 1 < len(cons) else {}
        end = None
        if not va or not vb:
            end = cal
        else:
            last_a = max(va)
            for d in sorted(set(va) | set(vb)):
                if (start and d < start) or d not in vb:
                    continue
                if (d in va and read(vb[d]) > read(va[d])) or (d not in va and d > last_a):
                    end = d
                    break
            if end is None:  # the next contract never traded more: this one stays front past its data
                end = max(max(va), max(vb)) + dt.timedelta(days=1)
        start = max(end, start) if start and end else end
        out.append((a, start))
    return out


@functools.lru_cache(maxsize=None)
def day_volume(path):
    return pc.sum(pq.ParquetFile(path).read(columns=["volume"])["volume"]).as_py() or 0


def volume_days(out, kind, inst):
    """{date: per-day Last parquet path} of one contract, or {} if it has none."""
    d = os.path.join(out, kind, "Last", inst.replace(" ", "_"))
    names = os.listdir(d) if os.path.isdir(d) else []
    return {dt.date(int(f[:4]), int(f[4:6]), int(f[6:8])): os.path.join(d, f)
            for f in names if re.fullmatch(r"\d{8}\.parquet", f)}


def session_start_us(day):
    """UTC microseconds of the session that trades on `day`: 18:00 ET the evening before."""
    # ponytail: fixed 18:00 ET break, right for CME Globex (daily halt 17:00-18:00 ET); other exchanges need their own
    t = dt.datetime.combine(day - dt.timedelta(days=1), dt.time(18), tzinfo=ncd.ET)
    return int(t.timestamp()) * 1_000_000


def windows(cons):
    """[(contract, start, stop)]: each contract owns [previous roll, own roll) (session days); None = open end."""
    return [(inst, cons[i - 1][1] if i else None, roll if i + 1 < len(cons) else None) for i, (inst, roll) in enumerate(cons)]


def trading_hours(nt8):
    """{futures root: trading-hours template name} from NT8's instrument database, opened read-only."""
    src = Path(nt8, "db", "NinjaTrader.sqlite").resolve()
    q = "select Name, TradingHours from MasterInstruments where InstrumentType = 0"  # 0 = InstrumentType.Future
    try:
        with sqlite3.connect(src.as_uri() + "?mode=ro", uri=True) as con:
            rows = con.execute(q).fetchall()
        con.close()
        return dict(rows)
    except sqlite3.OperationalError:  # locked or busy: read a private copy
        with tempfile.TemporaryDirectory() as tmp:
            for f in (src, src.with_name(src.name + "-wal")):
                if f.exists():
                    shutil.copy2(f, tmp)
            con = sqlite3.connect(os.path.join(tmp, src.name))
            try:
                return dict(con.execute(q).fetchall())
            finally:
                con.close()


@functools.lru_cache
def load_hours(path):
    return ncd.load_hours(path)


def stat(p):
    st = os.stat(p)
    return [st.st_size, st.st_mtime_ns]


def export_day(task):
    """Decode one output day (1 minute file or the day's hourly tick files) and write it. Runs in a pool worker."""
    srcs, dst, hours, ticks, tz = task
    try:
        before = [stat(p) for p in srcs]
        parts = []
        for p in srcs:
            try:
                parts.append(ncd.decode_file(p, tz))
            except Exception as e:  # FormatError, or a decoder bug: log it, keep the run going
                return "failed", f"{p}: {type(e).__name__}: {e}", 0
        if [stat(p) for p in srcs] != before:
            return "changed", "; ".join(srcs), 0
    except OSError as e:  # source vanished mid-read
        return "changed", str(e), 0
    cols = {k: np.concatenate([d[k] for d in parts]) for k in FIELDS}
    if hours:  # match NT8's export: keep only rows inside the instrument's trading-hours template
        keep = ncd.session_mask(cols["utc_us"], load_hours(hours), ticks)
        cols = {k: v[keep] for k, v in cols.items()}
    order = np.argsort(cols["utc_us"], kind="stable")
    table = pa.table({k: cols[k][order] for k in FIELDS}, schema=DAY_SCHEMA)
    pq.write_table(table, dst + ".tmp", compression="zstd")
    os.replace(dst + ".tmp", dst)
    return "ok", before, table.num_rows


def load_json(p):
    try:
        with open(p) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save_json(p, obj):
    with open(p + ".tmp", "w") as f:
        json.dump(obj, f)
    os.replace(p + ".tmp", p)


def plan(db, out, kind, typ, inst, force, hours, tz):
    """-> (tasks, manifest, manifest path, skipped count) for one contract/kind/type."""
    src_dir = os.path.join(db, kind, inst)
    dst_dir = os.path.join(out, kind, typ, inst.replace(" ", "_"))
    days = {}
    for name in sorted(os.listdir(src_dir)):
        if re.fullmatch(rf"\d{{8}}(\d{{4}})?\.{typ}\.ncd", name):  # skips stray copies like "202608310300 (1).Last.ncd"
            days.setdefault(name[:8], []).append(os.path.join(src_dir, name))  # tick: hourly names sort in hour order
    os.makedirs(dst_dir, exist_ok=True)
    mpath = os.path.join(dst_dir, "manifest.json")
    man = {} if force else load_json(mpath)
    tasks, skipped = [], 0
    for day, srcs in days.items():
        dst = os.path.join(dst_dir, day + ".parquet")
        try:
            cur = {os.path.basename(p): stat(p) for p in srcs}
        except OSError:
            continue  # vanished mid-scan: next run
        if not force and man.get(day) == cur and os.path.exists(dst):
            skipped += 1
        else:
            tasks.append((day, (srcs, dst, hours, kind == "tick", tz)))
    return tasks, man, mpath, skipped


def stitch(out, cons, kinds, types, csv, roll="volume"):
    """Continuous file per symbol: each contract's day files, cut to its window, streamed one day at a time.
    cons: {symbol: [(contract, roll date)]} as discover() returns it. roll="volume" picks the roll days from the
    per-day Last volumes of this kind (a missing day: the other kind's), so Bid/Ask roll on the same day as Last."""
    os.makedirs(os.path.join(out, "continuous"), exist_ok=True)
    for sym, cal_cons in cons.items():
        for kind in kinds:
            label = "1min" if kind == "minute" else "tick"
            sym_cons = cal_cons
            if roll == "volume":
                other = "tick" if kind == "minute" else "minute"  # fills days this kind is missing
                vol = {c: {**volume_days(out, other, c), **volume_days(out, kind, c)} for c, _ in cal_cons}
                sym_cons = pick_rolls(cal_cons, vol, day_volume)
            print(f"{sym} {kind} rolls: " + ", ".join(f"{c}->{d}" for (c, _), (_, d) in zip(cal_cons[1:], sym_cons)), flush=True)
            for t in types[kind]:
                dst = os.path.join(out, "continuous", f"{sym}_{label}_{t}")
                rows = unsorted = 0
                pw = pq.ParquetWriter(dst + ".parquet.tmp", CONT_SCHEMA, compression="zstd")
                gz = gzip.open(dst + ".csv.gz.tmp", "wb", compresslevel=6) if csv else None
                if gz:
                    gz.write(b"utc_us,open,high,low,close,volume,contract\n")
                for inst, start, stop in windows(sym_cons):
                    lo = session_start_us(start) if start else -2**63  # cut in the daily halt: no row lost or doubled
                    hi = session_start_us(stop) if stop else 2**63 - 1
                    d = os.path.join(out, kind, t, inst.replace(" ", "_"))
                    files = sorted(f for f in os.listdir(d) if f.endswith(".parquet")) if os.path.isdir(d) else []
                    # a day file may hold the next session's evening: read one day either side of the window
                    first = f"{start - dt.timedelta(days=1):%Y%m%d}" if start else ""
                    last = f"{stop + dt.timedelta(days=1):%Y%m%d}" if stop else "99999999"
                    prev = -1
                    for fn in files:
                        if not first <= fn[:8] <= last:
                            continue
                        tb = pq.ParquetFile(os.path.join(d, fn)).read()  # read_table() stats ~30 paths per call
                        u = tb["utc_us"]
                        tb = tb.filter(pc.and_(pc.greater_equal(u, lo), pc.less(u, hi)))
                        if not tb.num_rows:
                            continue
                        u = tb["utc_us"].to_numpy()
                        unsorted += int(u[0] < prev) + int((np.diff(u) < 0).sum())
                        prev = int(u[-1])
                        con = pa.DictionaryArray.from_arrays(pa.array(np.zeros(tb.num_rows, np.int32)), pa.array([inst]))
                        pw.write_table(tb.append_column(CONT_SCHEMA.field("contract"), con))
                        if gz:
                            # arrow's float->string is shortest round-trip without ".0": 1279.25 / 1279, as NT8 writes
                            txt = pa.table([tb["utc_us"]] + [pc.cast(tb[k], pa.string()) for k in FIELDS[1:5]]
                                           + [tb["volume"], con], names=list(CONT_SCHEMA.names))
                            pcsv.write_csv(txt, gz, CSV_OPTS)
                        rows += tb.num_rows
                pw.close()
                os.replace(dst + ".parquet.tmp", dst + ".parquet")
                if gz:
                    gz.close()
                    os.replace(dst + ".csv.gz.tmp", dst + ".csv.gz")
                print(f"{os.path.basename(dst)}: {rows:,} rows" + (f"  WARNING {unsorted} out-of-order rows" if unsorted else ""), flush=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--nt8-dir", default=os.environ.get("NT8_DIR") or str(Path.home() / "Documents" / "NinjaTrader 8"),
                    help="NT8 user folder, the one that holds db/ (default: env NT8_DIR, else ~/Documents/NinjaTrader 8)")
    ap.add_argument("--out", default="export_solo", help="output folder (default: ./export_solo)")
    ap.add_argument("--kinds", default="minute,tick")
    ap.add_argument("--types", help="default: minute Last,Bid,Ask; tick Last")
    ap.add_argument("--symbols", nargs="+", help="root symbols (default: every root with contract folders in db/)")
    ap.add_argument("--tz", default="America/New_York",
                    help="time zone NT8 is set to; .ncd times are local to it (default America/New_York). Changed it? run --force")
    ap.add_argument("--session", metavar="NAME", help="trading-hours template for every symbol (templates/TradingHours/NAME.xml) "
                                                       "instead of each instrument's own from NT8's instrument database")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--workers", type=int, default=os.cpu_count())
    ap.add_argument("--no-stitch", action="store_true")
    ap.add_argument("--raw", action="store_true", help="keep rows outside the trading-hours template (NT8's export drops them); use a separate --out")
    ap.add_argument("--roll", choices=("volume", "calendar"), default="volume",
                    help="volume (default): roll on the first day the next contract trades more; calendar: fixed date rule")
    ap.add_argument("--csv", action="store_true", help="also write each continuous file as csv.gz (header line, prices written like NT8)")
    a = ap.parse_args()
    kinds = a.kinds.split(",")
    types = {"minute": ["Last", "Bid", "Ask"], "tick": ["Last"]}
    if a.types:
        types = {k: a.types.split(",") for k in types}
    try:
        ncd.zone(a.tz)
    except Exception:  # ZoneInfoNotFoundError, or a bad key
        sys.exit(f"unknown time zone {a.tz!r}: use an IANA name like America/Chicago (on Windows: pip install tzdata)")
    db = os.path.join(a.nt8_dir, "db")
    cons = discover(db)
    if not cons:
        sys.exit(f"no contract folders like 'ES 12-26' under {db}/minute or {db}/tick: check --nt8-dir")
    syms = a.symbols or list(cons)
    for sym in syms:
        if sym not in cons:
            print(f"{sym}: no contract folders in {db}, skipped", flush=True)
    cons = {s: cons[s] for s in syms if s in cons}
    templates = None if a.raw or a.session else trading_hours(a.nt8_dir)
    os.makedirs(a.out, exist_ok=True)
    errlog = os.path.join(a.out, "errors.log")

    t0 = time.time()
    n = {"ok": 0, "skipped": 0, "failed": 0, "changed": 0, "rows": 0}
    groups, jobs = [], []  # groups[g] = [manifest, path, label, days left]; jobs = (g, day, (srcs, dst, ...))
    for sym, sym_cons in cons.items():
        hours = None
        if not a.raw:
            name = a.session or templates.get(sym)
            hours = os.path.join(a.nt8_dir, "templates", "TradingHours", f"{name}.xml")
            if not name or not os.path.exists(hours):
                why = f"template file {hours} is missing" if name else "no futures instrument of that name in db/NinjaTrader.sqlite"
                sys.exit(f"{sym}: cannot find its trading-hours template ({why}). "
                         f"Pass --session NAME (a file in templates/TradingHours) or --raw to keep every row.")
        for kind in kinds:
            for typ in types[kind]:
                for inst, _ in sym_cons:
                    if not os.path.isdir(os.path.join(db, kind, inst)):
                        continue
                    tasks, man, mpath, skipped = plan(db, a.out, kind, typ, inst, a.force, hours, a.tz)
                    n["skipped"] += skipped
                    groups.append([man, mpath, f"{inst} {kind} {typ}", len(tasks)])
                    save_json(mpath, man)
                    jobs += [(len(groups) - 1, day, tk) for day, tk in tasks]
    print(f"{len(jobs)} days to export, {n['skipped']} unchanged", flush=True)

    with mp.Pool(a.workers) as pool:
        for (g, day, (srcs, *_)), (status, info, rows) in zip(jobs, pool.imap(export_day, [j[2] for j in jobs])):
            man, mpath, label, _ = grp = groups[g]
            n[status] += 1
            n["rows"] += rows
            if status == "ok":
                man[day] = {os.path.basename(p): s for p, s in zip(srcs, info)}
            else:
                man.pop(day, None)
                msg = f"{dt.datetime.now():%Y-%m-%d %H:%M:%S} {status} {info}"
                print(msg, flush=True)
                if status == "failed":
                    with open(errlog, "a") as f:
                        f.write(msg + "\n")
            grp[3] -= 1
            if grp[3] == 0:  # contract done: persist its manifest
                save_json(mpath, man)
                print(f"{label}: done", flush=True)
    if not a.no_stitch:
        stitch(a.out, cons, kinds, types, a.csv, a.roll)
    print(f"decoded {n['ok']} days, skipped {n['skipped']} unchanged, changed-during-read {n['changed']}, "
          f"failed {n['failed']} (see {errlog}), {n['rows']:,} rows, {time.time() - t0:.0f}s", flush=True)
    return 1 if n["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
