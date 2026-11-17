#!/usr/bin/env python3
"""
aprs_gap_check.py - Find gaps in APRS-IS capture coverage in a Valkey stream.

Reads the `aprsis` stream (entries with `timestamp` and `packet` fields),
computes inter-arrival times over a chosen window, and reports every gap
longer than a threshold, plus summary statistics.

Requires:  pip install redis   (redis-py works fine against Valkey)

Usage examples:
  # Last 2 days, flag gaps > 120 seconds (defaults)
  python3 aprs_gap_check.py

  # Explicit window (UTC) and a 5-minute gap threshold
  python3 aprs_gap_check.py --start 2026-07-08T00:00:00 --end 2026-07-10T00:00:00 --gap 300

  # Different host/stream, dump all gaps to CSV
  python3 aprs_gap_check.py --host 127.0.0.1 --port 6379 --stream aprsis --csv gaps.csv
"""

import argparse
import csv
import re
import sys
from datetime import datetime, timedelta, timezone

try:
    import redis
except ImportError:
    sys.exit("redis-py is required:  pip install redis")

FRac = re.compile(r"\.(\d+)")

def parse_iso(ts: str) -> datetime:
    """Parse ISO-8601 with up to nanosecond precision and trailing Z."""
    ts = ts.strip().replace("Z", "+00:00")
    # Python's fromisoformat chokes on >6 fractional digits; trim to 6.
    ts = FRac.sub(lambda m: "." + m.group(1)[:6], ts, count=1)
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def parse_arg_time(s: str) -> datetime:
    """Parse a --start/--end argument; naive input is assumed UTC."""
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def fmt_dur(seconds: float) -> str:
    seconds = int(round(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h: return f"{h}h {m:02d}m {s:02d}s"
    if m: return f"{m}m {s:02d}s"
    return f"{s}s"

def iter_timestamps(r, stream, start, end, batch=5000):
    """Yield capture datetimes for entries in [start, end], in stream order."""
    cursor, hi = f"{ms(start)}-0", f"{ms(end)}-{2**64-1}"
    while True:
        entries = r.xrange(stream, min=cursor, max=hi, count=batch)
        if not entries:
            break
        for entry_id, fields in entries:
            ts_field = fields.get(b"timestamp") or fields.get("timestamp")
            if ts_field is None:
                # fall back to the stream ID's millisecond clock
                id_ms = int(str(entry_id).split("-")[0])
                yield datetime.fromtimestamp(id_ms / 1000, tz=timezone.utc)
            else:
                if isinstance(ts_field, bytes):
                    ts_field = ts_field.decode()
                yield parse_iso(ts_field)
        last_id = entries[-1][0]
        if isinstance(last_id, bytes):
            last_id = last_id.decode()
        cursor = "(" + last_id  # exclusive: resume after the last entry seen
        if len(entries) < batch:
            break

def main():
    ap = argparse.ArgumentParser(description="Detect capture gaps in a Valkey APRS stream")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--db", type=int, default=0)
    ap.add_argument("--stream", default="aprsis")
    ap.add_argument("--start", type=parse_arg_time, default=None,
                    help="window start, ISO-8601 UTC (default: end minus 2 days)")
    ap.add_argument("--end", type=parse_arg_time, default=None,
                    help="window end, ISO-8601 UTC (default: now)")
    ap.add_argument("--gap", type=float, default=120.0,
                    help="report gaps longer than this many seconds (default 120)")
    ap.add_argument("--csv", default=None, help="also write all reported gaps to this CSV file")
    args = ap.parse_args()

    end = args.end or datetime.now(timezone.utc)
    start = args.start or (end - timedelta(days=2))
    if start >= end:
        sys.exit("--start must be before --end")

    r = redis.Redis(host=args.host, port=args.port, db=args.db)

    gaps = []          # (gap_start, gap_end, seconds)
    count = 0
    prev = None
    first_ts = last_ts = None
    max_iat = 0.0      # largest inter-arrival time seen, even if under threshold

    for ts in iter_timestamps(r, args.stream, start, end):
        count += 1
        if first_ts is None:
            first_ts = ts
        if prev is not None:
            iat = (ts - prev).total_seconds()
            if iat > max_iat:
                max_iat = iat
            if iat > args.gap:
                gaps.append((prev, ts, iat))
        prev = ts
        last_ts = ts

    if count == 0:
        sys.exit(f"No entries found in {args.stream} between "
                 f"{start:%Y-%m-%d %H:%M:%S} and {end:%Y-%m-%d %H:%M:%S} UTC.")

    # Edge gaps: capture may not span the whole requested window
    lead = (first_ts - start).total_seconds()
    tail = (end - last_ts).total_seconds()

    span = (last_ts - first_ts).total_seconds()
    gap_total = sum(g[2] for g in gaps)
    covered = max(span - gap_total, 0)

    print(f"Stream          : {args.stream} on {args.host}:{args.port}")
    print(f"Window (UTC)    : {start:%Y-%m-%d %H:%M:%S} .. {end:%Y-%m-%d %H:%M:%S}")
    print(f"Packets         : {count}")
    print(f"First packet    : {first_ts:%Y-%m-%d %H:%M:%S}")
    print(f"Last packet     : {last_ts:%Y-%m-%d %H:%M:%S}")
    if span > 0:
        print(f"Avg rate        : {count / span * 60:.1f} packets/min")
    print(f"Largest interval: {fmt_dur(max_iat)}")
    print(f"Gap threshold   : {args.gap:.0f}s")
    print()

    if lead > args.gap:
        print(f"NOTE: no data for the first {fmt_dur(lead)} of the window "
              f"(capture starts {first_ts:%Y-%m-%d %H:%M:%S})")
    if tail > args.gap:
        print(f"NOTE: no data for the last {fmt_dur(tail)} of the window "
              f"(capture ends {last_ts:%Y-%m-%d %H:%M:%S})")

    if not gaps:
        print(f"No internal gaps longer than {args.gap:.0f}s. "
              f"Coverage looks continuous ({fmt_dur(covered)} of data).")
    else:
        print(f"{len(gaps)} gap(s) exceeding {args.gap:.0f}s "
              f"(total {fmt_dur(gap_total)}, "
              f"{gap_total / span * 100:.2f}% of the captured span):\n")
        print(f"{'#':>3}  {'gap start (UTC)':<20} {'gap end (UTC)':<20} {'duration':>10}")
        for i, (g0, g1, secs) in enumerate(sorted(gaps, key=lambda g: -g[2]), 1):
            print(f"{i:>3}  {g0:%Y-%m-%d %H:%M:%S}   {g1:%Y-%m-%d %H:%M:%S}   {fmt_dur(secs):>10}")

    if args.csv and gaps:
        with open(args.csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["gap_start_utc", "gap_end_utc", "duration_seconds"])
            for g0, g1, secs in gaps:
                w.writerow([g0.isoformat(), g1.isoformat(), f"{secs:.3f}"])
        print(f"\nGap list written to {args.csv}")

if __name__ == "__main__":
    main()
