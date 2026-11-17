#!/usr/bin/env python3
"""
find_gps_status.py - Show what status / comment text your stations actually send,
so you can find the exact GPS-loss wording to feed tracker_health.py
--gps-lost-pattern.

It scans the Valkey `aprsis` stream and reports, grouped by source callsign:
  * every distinct APRS *status* packet (info field starting with '>')
  * any packet whose text mentions gps / lock / lost / fix / nofix
with a count of how often each distinct string appeared.

Requires:  pip install redis

Usage:
  # everything in the window that looks GPS-related
  python3 find_gps_status.py --start 2026-07-10T00:00:00 --end 2026-07-11T23:59:59

  # just your Voyager trackers, ALL their status text (not only gps-ish)
  python3 find_gps_status.py --calls W1CMS-1,W1CMS-9,W1HS-9 --all-status
"""

import argparse
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

try:
    import redis
except ImportError:
    sys.exit("redis-py is required:  pip install redis")

FRAC = re.compile(r"\.(\d+)")
GPSISH = re.compile(r"gps|lock|lost|no\s*fix|nofix|acq|sat", re.I)

def parse_iso(ts):
    ts = ts.strip().replace("Z", "+00:00")
    ts = FRAC.sub(lambda m: "." + m.group(1)[:6], ts, count=1)
    dt = datetime.fromisoformat(ts)
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)

def parse_arg_time(s):
    dt = datetime.fromisoformat(s)
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)

def ms(dt):
    return int(dt.timestamp() * 1000)

def iter_stream(r, stream, start, end, batch=5000):
    cursor, hi = f"{ms(start)}-0", f"{ms(end)}-{2**64-1}"
    while True:
        entries = r.xrange(stream, min=cursor, max=hi, count=batch)
        if not entries:
            break
        for entry_id, fields in entries:
            kb = isinstance(next(iter(fields)), bytes)
            def g(k):
                v = fields.get(k.encode()) if kb else fields.get(k)
                return v.decode() if isinstance(v, bytes) else v
            pkt = g("packet")
            if pkt:
                yield pkt
        last_id = entries[-1][0]
        if isinstance(last_id, bytes):
            last_id = last_id.decode()
        cursor = "(" + last_id
        if len(entries) < batch:
            break

def main():
    ap = argparse.ArgumentParser(description="Find status/GPS text stations send")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--db", type=int, default=0)
    ap.add_argument("--stream", default="aprsis")
    ap.add_argument("--calls", default="", help="restrict to these sources (comma-separated)")
    ap.add_argument("--start", type=parse_arg_time, default=None)
    ap.add_argument("--end", type=parse_arg_time, default=None)
    ap.add_argument("--all-status", action="store_true",
                    help="show ALL status packets, not just gps-related text")
    args = ap.parse_args()

    end = args.end or datetime.now(timezone.utc)
    start = args.start or (end - timedelta(days=2))
    want = {c.strip().upper() for c in args.calls.split(",") if c.strip()}

    r = redis.Redis(host=args.host, port=args.port, db=args.db)
    # per source: Counter of distinct interesting strings
    status_by = defaultdict(Counter)   # ">..." status packets
    gpsish_by = defaultdict(Counter)   # any text mentioning gps/lock/lost/etc.
    scanned = 0
    for raw in iter_stream(r, args.stream, start, end):
        scanned += 1
        if ">" not in raw or ":" not in raw:
            continue
        src = raw.split(">", 1)[0].upper()
        if want and src not in want:
            continue
        info = raw.split(":", 1)[1].strip()
        if not info:
            continue
        if info.startswith(">"):                      # APRS status report
            text = info[1:].strip()
            if args.all_status or GPSISH.search(text):
                status_by[src][text] += 1
        if GPSISH.search(info):                       # gps-ish anywhere in info
            gpsish_by[src][info] += 1

    print(f"Scanned {scanned} entries, window {start:%Y-%m-%d %H:%M}-{end:%Y-%m-%d %H:%M}z\n")

    if status_by:
        print("=== STATUS packets (info starts with '>') ===")
        for src in sorted(status_by):
            print(f"\n{src}:")
            for text, n in status_by[src].most_common():
                print(f"  x{n:<5} >{text}")
    else:
        print("No matching status packets found.")

    # gps-ish that were NOT plain status (e.g. embedded in a comment)
    extra = {src: c for src, c in gpsish_by.items()
             if any(not_ for not_ in [True])}
    print("\n=== any info text mentioning gps/lock/lost/fix (incl. comments) ===")
    any_found = False
    for src in sorted(gpsish_by):
        # skip lines already shown as pure status to reduce noise
        lines = [(t, n) for t, n in gpsish_by[src].most_common()
                 if not t.startswith(">")]
        if not lines:
            continue
        any_found = True
        print(f"\n{src}:")
        for text, n in lines[:8]:
            print(f"  x{n:<5} {text[:100]}")
    if not any_found:
        print("(none beyond the status packets above)")

    print("\nOnce you see the exact wording, pass it to tracker_health.py, e.g.:")
    print('  --gps-lost-pattern "gps.*lost"    (case-insensitive, regex)')

if __name__ == "__main__":
    main()
