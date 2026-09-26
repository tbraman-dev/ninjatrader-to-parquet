# ninjatrader-to-parquet

Read NinjaTrader 8 tick and minute history (`.ncd` files) in Python. Export it to Parquet or CSV **without NinjaTrader running**.

```bash
pip install numpy pyarrow tzdata
python export_ncd.py --symbols ES NQ
```

```python
import pandas as pd
df = pd.read_parquet("export_solo/continuous/ES_1min_Last.parquet")
```

## Why this exists

NinjaTrader 8 is a good way to get futures history. Your data feed fills its local database with years of minute bars and months of tick data. But that data is stuck in NinjaTrader's own binary format, `.ncd`, and there is no public spec for it.

The official ways to get it out are slow and manual:

- **Historical Data window → Export:** one instrument, one data type and one date range at a time. It works for a few files. It does not work for 20 years × 5 symbols × 3 data types.
- **A NinjaScript exporter:** runs inside NinjaTrader, so NinjaTrader must be open. Speed is about one day of data per second. It holds up the platform while it works, and it needs its own request queue.

If you backtest in Python (pandas, polars, vectorbt, your own code), you want the data **as files**: all of it, in one command, again every night, with no GUI.

This tool reads the `.ncd` files directly:

- **No NinjaTrader needed.** It only reads the `db` folder. You can copy the folder to another PC (Windows, Linux or Mac) and export there.
- **Fast.** About 2 million rows per second per core, and it uses every core. Months of tick data for several symbols take minutes, not hours.
- **Incremental.** It keeps a manifest of each source file's size and change time. A second run exports only the days that changed. That makes it safe to run every night.
- **Exact.** The output matches NinjaTrader's own export row for row: same timestamps, same prices (compared in whole ticks) and same volumes. See [Correctness](#correctness).
- **Honest about gaps.** An encoding it has not seen proven stops with an error that names the file and byte offset. It does not guess.

## Companion to nt8-mcp

[nt8-mcp](https://github.com/tbraman-dev/nt8-mcp) lets an AI agent drive NinjaTrader 8: download history, run backtests, read charts. This repo is its offline other half:

1. **nt8-mcp** (NinjaTrader running) fills the database with `nt_data_download` from your connected data provider.
2. **ninjatrader-to-parquet** (NinjaTrader optional) turns that database into Parquet for Python research.

You do not need nt8-mcp to use this tool. Any NinjaTrader 8 database works, however it was filled.

## Install

Python 3.10+ with:

```bash
pip install numpy pyarrow tzdata
```

`tzdata` is only needed on Windows, where Python has no time zone database of its own. There is nothing else to install: 2 files, no package.

## Usage

```bash
python export_ncd.py [options]
```

| Option | Default | What it does |
|---|---|---|
| `--nt8-dir DIR` | env `NT8_DIR`, else `~/Documents/NinjaTrader 8` | The NinjaTrader 8 user folder. The one that holds `db/`. |
| `--out DIR` | `export_solo` | Where the output goes. A relative path starts in the folder you run from. |
| `--symbols ES NQ …` | every root found in `db/` | Root symbols to export. A root is found from its contract folders, for example `ES 12-26`. |
| `--kinds minute,tick` | both | Which stores to read. |
| `--types Last,Bid,Ask` | minute: all 3, tick: `Last` | Data types. |
| `--tz ZONE` | `America/New_York` | The time zone NinjaTrader is set to (Tools → Options → General). `.ncd` times are stored in that local time. If you change it, run with `--force`. |
| `--session NAME` | each instrument's own | The trading-hours template to use for every symbol: `templates/TradingHours/NAME.xml`. |
| `--workers N` | all cores | Worker processes. Use fewer if NinjaTrader is running on the same PC. |
| `--force` | off | Export every day again and ignore the manifest. |
| `--raw` | off | Keep every bar and tick. Without it, rows outside the instrument's trading hours are dropped: the daily halt, weekends, holidays and early closes. NinjaTrader's own export does the same. Use a separate `--out`. |
| `--no-stitch` | off | Write the per-day files only. |
| `--csv` | off | Also write each continuous file as `.csv.gz`. |

Examples:

```bash
# everything, every symbol found
python export_ncd.py

# ES and NQ minute bars only, from a database copied off another PC
python export_ncd.py --nt8-dir /mnt/backup/NinjaTrader\ 8 --symbols ES NQ --kinds minute

# nightly top-up while NinjaTrader is open: leave it some CPU
python export_ncd.py --workers 8
```

### Trading hours

The tool reads each instrument's trading-hours template name from NinjaTrader's instrument database, `db/NinjaTrader.sqlite`. It opens that file read-only. Then it loads the template from `templates/TradingHours/`. If it cannot find a template for a symbol, it stops and tells you to pass `--session NAME` or `--raw`.

## Output

```
export_solo/
  minute/Last/ES_12-26/20260925.parquet      one file per contract per day
  tick/Last/ES_12-26/20260925.parquet
  continuous/ES_1min_Last.parquet            one file per symbol: contracts joined at the roll
  continuous/ES_tick_Last.parquet
  errors.log                                 files that could not be decoded (the run continues)
```

Columns:

| Column | Type | Meaning |
|---|---|---|
| `utc_us` | int64 | UTC time in microseconds since 1970. Minute bars are stamped at the bar **close**, the same as in NinjaTrader. A tick has its own time. |
| `open`, `high`, `low`, `close` | float64 | Rounded to the instrument's tick size. For a tick, all four are the trade (or bid/ask) price. |
| `volume` | int64 | |
| `contract` | string | Continuous files only: the contract each row came from, for example `ES 12-26`. |

Continuous files are **not back-adjusted**. The `contract` column tells you where each roll is, so you can adjust in your own code. The roll day is approximate: 8 days before the third Friday of the expiry month. For metals (GC, SI, HG, PL, PA, MGC, SIL), the 26th of the month before expiry. Each contract fills the continuous file from the previous contract's roll day to its own roll day, cut at UTC midnight. The first contract has no start limit and the last has no end limit. The per-day files are always exact. Only the choice of which contract fills a continuous file near a roll is a rule of thumb.

## Where NinjaTrader keeps the data

```
Documents/NinjaTrader 8/db/minute/<CONTRACT>/<YYYYMMDD>.<Last|Bid|Ask>.ncd         one file per day
Documents/NinjaTrader 8/db/tick/<CONTRACT>/<YYYYMMDDHH00>.<Last|Bid|Ask>.ncd       one file per hour
```

Dates and hours in the file names are in NinjaTrader's local time zone. A tick file named `…HH00` holds the hour that **ends** at HH:00. Stray copies such as `202608310300 (1).Last.ncd` are skipped. Empty contract folders are ignored.

## Correctness

The decoder was built against NinjaTrader's own exports of the same files. Every test file matches row for row on count, time, prices in whole ticks, and volume:

- minute Last, Bid and Ask: ES and GC from 2026, and ES from 2010, an older format with Last only
- tick Last and Bid: ES and NQ from 2026, about 1.2 million ticks per day
- daylight saving time edges in 2006 (the old US rule) and 2026

The test data is not in this repo. It is exchange market data, and your data license almost certainly does not allow it to be shared. To check your own setup, export a few days from NinjaTrader (Historical Data → Export) and compare them with this tool's output for the same days.

## File format notes

`.ncd` is a compact delta encoding:

- **Header:** 28 bytes, little-endian. `int32 version`, `float64 tick_size`, `float64 first_price`, `int64 first_time` (.NET ticks in local time).
- **Records:** variable length and big-endian. Each record starts with 2 flag bytes. They say how many bytes follow for the time step, the price change (in ticks), and the volume.
- **Minute records:** also carry high, low and close as tick offsets.

The full bit layout is in the docstring at the top of [`ncd.py`](ncd.py). Only the size codes seen in real files are turned on. Any other code raises `FormatError` with the file, the byte offset, and 16 bytes of hex. If you hit one, please open an issue with that line, and with the NinjaTrader export of that day if you can.

## Limits

- **Reading only.** It never writes to the NinjaTrader folder. If NinjaTrader is still downloading, a file can change while it is being read. That day is skipped and exported on the next run.
- **Minute and tick stores only.** Daily bars and Market Replay (`.nrd`) files are not covered. For `.nrd`, nt8-mcp has an offline decoder.
- **One time zone per database.** Set `--tz` to match NinjaTrader's setting.
- **Not affiliated with NinjaTrader.** NinjaTrader is a trademark of NinjaTrader, LLC. This is an independent tool for reading your own data.

## Credits

The format work builds on [NinjaTraderNCDFiles](https://github.com/jrstokka/NinjaTraderNCDFiles) by John R. Stokka (MIT). It was cross-checked with [NTDFileReader](https://github.com/bboyle1234/NTDFileReader). Everything here was then checked row for row against NinjaTrader's own exports.

## License

MIT. See [LICENSE](LICENSE).
