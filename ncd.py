"""Pure-Python decoder for NinjaTrader 8 .ncd history files (minute + tick).

Format facts ported from JR Stokka's NCDFile.cs
(https://github.com/jrstokka/NinjaTraderNCDFiles, MIT License, (c) 2019 John R. Stokka)
and cross-read against https://github.com/bboyle1234/NTDFileReader (NCDUtility.cs).
Only size codes seen in real files are enabled (all row-exact vs NT8's own export, except
tick time code 4, which is checked structurally: rows land in the file's hour); any other
code raises FormatError instead of guessing.

Header (28 bytes, little-endian): i32 version=1 | f64 tick_size | f64 first_price |
i64 t0 (.NET ticks, local time of the NT8 install). Body: variable-length records, big-endian.

Minute record: b1 b2 [time] [open] [high] [low] [close] volume
  b1 & 0x03 time: 0 = +1 minute, 1 = +u8 minutes, 2 = +u16 minutes (first record: +0)
  b1 >> 2 & 3 open delta vs previous open: 0 = same, 1 = u8 - 0x80, 2 = u16 - 0x8000
  b2 >> 4 & 3 high (h - o), b2 >> 6 low (o - l), b2 & 3 close (c - l): 0 = 0, 1 = u8, 2 = u16, 3 = u32
  b1 >> 5 volume code (MIN_VOL below)
Tick record: b1 b2 [time] [price] [spread] volume
  b1 & 0x07 time delta: 0 none, 1 u8, 2 u16, 3 u32, 4 u64 (100 ns units), 5 u8 seconds, 6 u16 seconds
  b1 >> 6 price delta: 0 none, 1 = (b2 & 0x1F) - 16, 2 = u8 - 0x80, 3 = u32 - 0x80000000
  b1 >> 3 & 7 bid/ask spread info (skipped): 6 = one extra byte, 7 = two, else none
  b2 >> 5 volume code (TICK_VOL below)
Timestamps are the bar CLOSE time as NT8 stamps it (minute) / the tick time.
"""
import re
import struct
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np

EPOCH_TICKS = 621355968000000000  # .NET ticks at 1970-01-01T00:00
TICKS_PER_MIN = 600_000_000
HOUR_US = 3_600_000_000
ET = ZoneInfo("America/New_York")
_EPOCH = datetime(1970, 1, 1)
# volume code -> (byte count, multiplier); only codes proven on real files.
# Prior art also has 7 = u64 (and 3/4/6 for ticks): add once a real file shows them.
MIN_VOL = {1: (1, 1), 2: (1, 100), 3: (1, 500), 4: (1, 1000), 5: (2, 1), 6: (4, 1)}
HLC_SZ = (0, 1, 2, 4)  # minute high/low/close offset code -> big-endian byte count
TICK_VOL = {1: (1, 1), 2: (1, 100), 5: (2, 1)}
TICK_TS = {0: 0, 1: 1, 2: 2, 3: 4, 4: 8}


class FormatError(RuntimeError):
    pass


def _bad(name, buf, off, what):
    raise FormatError(f"{name}: {what} at byte {off}: {buf[off:off + 16].hex(' ')}")


def _decimals(tick):
    return max(0, -Decimal(str(tick)).as_tuple().exponent)


def decode_file(path, tz=None):
    """Decode one .ncd file -> dict of numpy columns. tz: the zone NT8 runs in (ZoneInfo or name; default US Eastern)."""
    path = Path(path)
    stem = path.name.split(".")[0]
    if re.fullmatch(r"\d{8}", stem):
        minute = True
    elif re.fullmatch(r"\d{12}", stem):
        minute = False
    else:
        raise FormatError(f"{path.name}: cannot tell minute from tick by name")
    buf = path.read_bytes()
    if len(buf) < 28:
        raise FormatError(f"{path.name}: shorter than the 28-byte header")
    ver, tick, first, t0 = struct.unpack_from("<iddq", buf, 0)
    if ver != 1:
        _bad(path.name, buf, 0, f"version {ver}")
    p0 = round(first / tick)
    try:
        cols = (_minute if minute else _tick)(buf, path.name, t0, p0)
    except IndexError:
        raise FormatError(f"{path.name}: record runs past end of file ({len(buf)} bytes)") from None
    t, o, h, l, c, v = (np.array(x, dtype=np.int64) for x in cols)
    nd = _decimals(tick)
    out = {"utc_us": to_utc_us(t, tz), "volume": v}
    for k, a in (("open", o), ("high", h), ("low", l), ("close", c)):
        out[k] = np.round(a * tick, nd)
    return out


def _minute(buf, name, t, p):
    n = len(buf); i = 28
    T = []; O = []; H = []; L = []; C = []; V = []
    while i < n:
        b1 = buf[i]; b2 = buf[i + 1]; j = i + 2
        tc = b1 & 3
        if tc == 0:
            t += TICKS_PER_MIN
        elif tc == 1:
            t += buf[j] * TICKS_PER_MIN; j += 1
        elif tc == 2:
            t += (buf[j] << 8 | buf[j + 1]) * TICKS_PER_MIN; j += 2
        else:
            _bad(name, buf, i, f"minute time code {tc}")
        oc = (b1 >> 2) & 3
        if oc == 1:
            p += buf[j] - 0x80; j += 1
        elif oc == 2:
            p += (buf[j] << 8 | buf[j + 1]) - 0x8000; j += 2
        elif oc:
            _bad(name, buf, i, f"minute open code {oc}")
        if b2 & 0x0C or b2 >> 6 == 3:  # low code 3 (u32) never seen yet
            _bad(name, buf, i, f"minute b2 {b2:#04x}")
        sz = HLC_SZ[(b2 >> 4) & 3]
        hi = p + int.from_bytes(buf[j:j + sz], "big"); j += sz
        sz = HLC_SZ[b2 >> 6]
        lo = p - int.from_bytes(buf[j:j + sz], "big"); j += sz
        sz = HLC_SZ[b2 & 3]
        cl = lo + int.from_bytes(buf[j:j + sz], "big"); j += sz
        vc = MIN_VOL.get(b1 >> 5)
        if vc is None:
            _bad(name, buf, i, f"minute volume code {b1 >> 5}")
        sz, mul = vc
        v = int.from_bytes(buf[j:j + sz], "big") * mul; j += sz
        if j > n:
            raise IndexError
        T.append(t); O.append(p); H.append(hi); L.append(lo); C.append(cl); V.append(v)
        i = j
    return T, O, H, L, C, V


def _tick(buf, name, t, p):
    n = len(buf); i = 28
    T = []; P = []; V = []
    tsz = TICK_TS; vol = TICK_VOL
    while i < n:
        b1 = buf[i]; b2 = buf[i + 1]; j = i + 2
        sz = tsz.get(b1 & 7)
        if sz:
            t += int.from_bytes(buf[j:j + sz], "big"); j += sz
        elif sz is None:
            if b1 & 7 == 5:  # u8 seconds
                t += buf[j] * 10_000_000; j += 1
            elif b1 & 7 == 6:  # u16 seconds
                t += (buf[j] << 8 | buf[j + 1]) * 10_000_000; j += 2
            else:
                _bad(name, buf, i, f"tick time code {b1 & 7}")
        pc = b1 >> 6
        if pc == 1:
            p += (b2 & 0x1F) - 16
        elif pc == 2:
            p += buf[j] - 0x80; j += 1
        elif pc == 3:
            p += int.from_bytes(buf[j:j + 4], "big") - 0x80000000; j += 4
        sp = (b1 >> 3) & 7
        if sp >= 6:
            j += sp - 5  # 6: one spread byte, 7: two
        vc = vol.get(b2 >> 5)
        if vc is None:
            _bad(name, buf, i, f"tick volume code {b2 >> 5}")
        sz, mul = vc
        v = int.from_bytes(buf[j:j + sz], "big") * mul; j += sz
        if j > n:
            raise IndexError
        T.append(t); P.append(p); V.append(v)
        i = j
    return T, P, P, P, P, V


_DAYS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
# Windows zone names used by NT8's trading-hours templates -> IANA
_WIN_TZ = {"Eastern Standard Time": "America/New_York", "Central Standard Time": "America/Chicago",
           "GMT Standard Time": "Europe/London", "Central Europe Standard Time": "Europe/Budapest",
           "W. Europe Standard Time": "Europe/Berlin", "China Standard Time": "Asia/Shanghai",
           "Tokyo Standard Time": "Asia/Tokyo", "AUS Eastern Standard Time": "Australia/Sydney",
           "India Standard Time": "Asia/Kolkata", "Singapore Standard Time": "Asia/Singapore",
           "Arab Standard Time": "Asia/Riyadh", "E. South America Standard Time": "America/Sao_Paulo",
           "UTC": "UTC"}


def zone(tz):
    """ZoneInfo from an IANA name, a Windows zone name as NT8 writes it, or a ZoneInfo; None = US Eastern."""
    if tz is None:
        return ET
    return tz if isinstance(tz, ZoneInfo) else ZoneInfo(_WIN_TZ.get(tz, tz))
DAY_US = 24 * HOUR_US


def load_hours(path):
    """Parse an NT8 trading-hours template (templates/TradingHours/<name>.xml)."""
    import xml.etree.ElementTree as ElementTree
    root = ElementTree.parse(path).getroot().find("TradingHours")
    hm = lambda e, k: (int(e.findtext(k)) // 100 * 60 + int(e.findtext(k)) % 100) * 60_000_000
    wd = lambda e, k: _DAYS.index(e.findtext(k))
    day = lambda e: (datetime.fromisoformat(e.findtext("Date")) - _EPOCH).days
    sess = []
    for e in root.find("Sessions"):
        b, en = wd(e, "BeginDay") * DAY_US + hm(e, "BeginTime"), wd(e, "EndDay") * DAY_US + hm(e, "EndTime")
        if b >= en:
            raise FormatError(f"{path}: session wraps the week, not supported")
        sess.append((b, en, wd(e, "TradingDay")))
    partial = {}
    for e in root.find("PartialHolidaysSerializable"):
        c, d = e.find("Constraint"), day(e)
        back = lambda k: (((d + 4) % 7 - wd(c, k)) % 7) * DAY_US  # weekday of trading date d back to the constraint day
        end = d * DAY_US - back("EndDay") + hm(c, "EndTime") if e.findtext("IsEarlyEnd") == "true" else None
        beg = d * DAY_US - back("BeginDay") + hm(c, "BeginTime") if e.findtext("IsLateBegin") == "true" else None
        partial[d] = (beg, end)
    return {"tz": zone(root.findtext("TimeZone")), "sessions": sess,
            "holidays": np.array([day(e) for e in root.find("HolidaysSerializable")], np.int64), "partial": partial}


def session_mask(utc_us, hours, ticks=False):
    """Bool mask of rows NT8's export keeps under a trading-hours template: inside a session
    (begin, end], trading date not a holiday, and inside any early-end / late-begin partial holiday.
    ticks=True: a tick stamped exactly at the session begin is kept, [begin, end] (a minute bar
    stamped at the begin covers the minute before it, so bars use (begin, end])."""
    u = np.asarray(utc_us, dtype=np.int64)
    hrs, inv = np.unique(u // HOUR_US, return_inverse=True)
    off = np.array([datetime.fromtimestamp(int(h) * 3600, hours["tz"]).utcoffset() // timedelta(microseconds=1)
                    for h in hrs], dtype=np.int64)
    loc = u + off[inv].reshape(u.shape)  # template-local microseconds since 1970-01-01
    day = loc // DAY_US
    wd = (day + 4) % 7  # Sunday = 0 (1970-01-01 was a Thursday)
    mow = wd * DAY_US + loc % DAY_US
    keep = np.zeros(u.shape, bool)
    tdate = np.zeros(u.shape, np.int64)
    for b, e, twd in hours["sessions"]:
        m = ((mow >= b) if ticks else (mow > b)) & (mow <= e)
        keep |= m
        tdate[m] = day[m] + (twd - wd[m]) % 7
    keep &= ~np.isin(tdate, hours["holidays"])
    for d in np.intersect1d(tdate[keep], np.fromiter(hours["partial"], np.int64)):
        beg, end = hours["partial"][int(d)]
        m = tdate == d
        if end is not None:
            keep &= ~(m & (loc > end))
        if beg is not None:
            keep &= ~(m & ((loc < beg) if ticks else (loc <= beg)))
    return keep


def to_utc_us(local_ticks, tz=None):
    """.NET ticks in NT8's local time (tz, default US Eastern) -> UTC microseconds (int64 array).
    The repeated fall-back hour is read as standard time (fold=1), like .NET
    TimeZoneInfo.ConvertTimeToUtc; a spring-forward gap time is read as daylight time."""
    # ponytail: offsets looked up once per local hour; zones with :30/:45 DST shifts would need per-minute lookup
    tz = zone(tz)
    us = (np.asarray(local_ticks, dtype=np.int64) - EPOCH_TICKS) // 10
    hrs, inv = np.unique(us // HOUR_US, return_inverse=True)
    off = [(_EPOCH + timedelta(hours=int(h))).replace(fold=1, tzinfo=tz).utcoffset() for h in hrs]
    off = np.array([o // timedelta(microseconds=1) for o in off], dtype=np.int64)
    return us - off[inv].reshape(us.shape) if len(hrs) else us


def et_to_utc_us(local_ticks):
    """US Eastern shortcut for to_utc_us."""
    return to_utc_us(local_ticks, ET)
