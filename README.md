# Prouty-APRS-Tools

Post-event APRS log analysis for **The Prouty** (Twin State Radio Club / W1FN,
West Central NH ARES). A small set of standalone Python tools that replay a
captured APRS-IS packet stream to produce coverage maps, rover tracks, and
tracker health / telemetry reports.

Built for the 2026 Prouty (Ultimate: Fri Jul 10; Bike Event: Sat Jul 11), but
nothing is hard-coded to that date — point the tools at any window.

## What it does

The tools fall into two buckets, plus diagnostics.

### Coverage & mapping
| Tool | Purpose |
|------|---------|
| `digi_kmz.py` | Per-digipeater/igate coverage footprints as KMZ. One color per station (dots + home ring), field traffic separated from infrastructure beacons, igates credited only for RF-heard packets (`qAR`/`qAO`). |
| `prouty_kmz.py` | Per-callsign track export, aprs.fi-style KMZ, straight from the capture DB (no download quota). |
| `add_packet_dots.py` | Post-processes any track KMZ to add a dot at every received packet, so gaps in the dots reveal coverage holes. Dot icon is embedded so it renders in Google Earth. |
| `aprs_gap_check.py` | Finds gaps in the *capture pipeline itself* (so you don't mistake a capture outage for a coverage hole). |
| `aprs2kmz.py` | Converts raw APRS packets (aprs.fi raw-page paste, APRS-IS log, or bare TNC2) to a KMZ. Standalone, does not need the database. |

### Tracker health & telemetry
| Tool | Purpose |
|------|---------|
| `tracker_health.py` | Per-tracker anomaly report: `GPSLOST` (Voyager "GPS Lock Lost"/"GPS Locked" status), `FROZEN`, `JUMP`, `DROPOUT`, `NOFIX`, `STALEDEFAULT`, `LOWVOLT`, `PARSEFAIL`. Console report + summary CSV + event CSV + optional anomaly KMZ. |
| `telemetry_decode.py` | Decodes `T#` telemetry to engineering units using each station's own `PARM`/`UNIT`/`EQNS` definitions discovered in the stream (with empirical / `--eqns` fallbacks). Reports per-tracker battery voltage & temperature. |

### Diagnostics
| Tool | Purpose |
|------|---------|
| `find_gps_status.py` | Dumps the distinct status/comment text each station sends — used to find the exact GPS-loss wording. |
| `parsefail_probe.py` | Categorizes what `aprslib` fails to parse (by APRS data type and error), so a `PARSEFAIL` count can be judged harmless (e.g. `T#` telemetry) vs. real. |
| `stream_grep.py` | Finds packets mentioning a string in the stream and shows how they parse. |

## Requirements

- Python 3.9+
- `redis` (the redis-py client talks to Valkey natively)
- `aprslib` — APRS packet parsing
- `pillow` — only for tools that embed map icons (`digi_kmz.py`, `add_packet_dots.py`, `tracker_health.py --kmz`)

```
pip install -r requirements.txt
```

> Note: `aprslib` 0.7.2 raises `UnknownFormat` on `T#` telemetry packets. That is
> expected and handled — `tracker_health.py` skips them rather than counting them
> as malformed, and `telemetry_decode.py` parses `T#` itself.

## Data source

The tools read a [Valkey](https://valkey.io/) **stream** named `aprsis` (override
with `--stream`), where each entry has two fields:

```
timestamp : ISO-8601 UTC, e.g. 2026-07-07T22:34:26.3134021Z
packet    : the raw APRS-IS line, e.g.
            W1CMS-8>APMI0A,WIDE1-1,qAR,W1FN-5:!4342.22N/07217.30W>030/045
```

Capture is whatever feeds that stream (an APRS-IS client writing with `XADD`).
Default connection is `127.0.0.1:6379`, db `0`.

## Quick start

```bash
# Rover coverage footprints for the event window
python3 tools/digi_kmz.py \
  --calls N1GMC-1,N1GMC-2,W1FN-9,W1FN-5,W1FN-1,W1FN-3,WX1NH-10 \
  --start 2026-07-10T00:00:00 --end 2026-07-11T23:59:59 \
  --out-dir out/

# Rover tracks, then add per-packet dots
python3 tools/prouty_kmz.py \
  --calls W1CMS-7,W1CMS-8,W1CMS-9,W1CMS-10,W1CMS-11 \
  --start 2026-07-10T00:00:00 --end 2026-07-11T23:59:59 --out-dir out/
python3 tools/add_packet_dots.py out/ -o out/dots/

# Tracker health + telemetry
python3 tools/tracker_health.py \
  --calls W1CMS-7,W1CMS-8,W1CMS-9,W1CMS-10,W1CMS-11 \
  --start 2026-07-10T00:00:00 --end 2026-07-11T23:59:59 \
  --min-voltage 11.5 --kmz --out-dir out/
python3 tools/telemetry_decode.py \
  --calls W1CMS-7,W1CMS-8,W1CMS-9,W1CMS-10,W1CMS-11 \
  --start 2026-07-10T00:00:00 --end 2026-07-11T23:59:59 \
  --min-voltage 11.5 --csv --out-dir out/
```

All times are UTC unless you include an offset (e.g. `...T00:00:00-04:00`).
Each tool has `--help`.

## 2026 findings

A written summary of the 2026 results (coverage, rover tracks, GPS-loss
behavior of the new Microsat Voyager trackers, and battery telemetry) is in
[`docs/aprs-analysis.html`](docs/aprs-analysis.html) — the APRS section of the
Prouty 2026 After Action Records.

## Open items

- `tracker_health.py` — suppress sub-2-second `JUMP` steps (same-timestamp
  duplicate packets produce nonsense implied speeds).
- `tracker_health.py` — auto-classify each `DROPOUT` as *tracker fault* vs
  *coverage hole* by cross-referencing the digi/igate reception data.

## License

MIT — see [LICENSE](LICENSE).
