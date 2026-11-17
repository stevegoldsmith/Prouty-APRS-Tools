#!/usr/bin/env python3
"""
stream_grep.py - Find packets mentioning a string in the Valkey aprsis stream
and show how they parse. Diagnostic companion to prouty_kmz.py.

Usage:
  python3 stream_grep.py W1CMS-2
  python3 stream_grep.py W1CMS-2 --start 2026-07-10T00:00:00 --end 2026-07-11T23:59 --limit 20
"""

import argparse
import re
import sys
from datetime import datetime, timezone

try:
    import redis
except ImportError:
    sys.exit("pip install redis")
try:
    import aprslib
    HAVE_APRSLIB = True
except ImportError:
    HAVE_APRSLIB = False

FRAC = re.compile(r"\.(\d+)")

def parse_arg_time(s):
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("needle", help="substring to search for, e.g. W1CMS-2")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--db", type=int, default=0)
    ap.add_argument("--stream", default="aprsis")
    ap.add_argument("--start", type=parse_arg_time, default=None)
    ap.add_argument("--end", type=parse_arg_time, default=None)
    ap.add_argument("--limit", type=int, default=10, help="max examples to print")
    args = ap.parse_args()

    r = redis.Redis(host=args.host, port=args.port, db=args.db)
    lo = f"{int(args.start.timestamp()*1000)}-0" if args.start else "-"
    hi = f"{int(args.end.timestamp()*1000)}-{2**64-1}" if args.end else "+"

    needle = args.needle.upper()
    total = as_source = with_pos = shown = 0
    cursor = lo
    while True:
        entries = r.xrange(args.stream, min=cursor, max=hi, count=5000)
        if not entries:
            break
        for entry_id, fields in entries:
            pkt = fields.get(b"packet") or fields.get("packet")
            if pkt is None:
                continue
            if isinstance(pkt, bytes):
                pkt = pkt.decode(errors="replace")
            if needle not in pkt.upper():
                continue
            total += 1
            ts = fields.get(b"timestamp") or fields.get("timestamp") or b""
            if isinstance(ts, bytes):
                ts = ts.decode()
            src = pkt.split(">", 1)[0].upper()
            is_src = src == needle
            as_source += is_src
            verdict = ""
            if HAVE_APRSLIB:
                try:
                    p = aprslib.parse(pkt)
                    if "latitude" in p:
                        with_pos += 1
                        verdict = f"-> parsed OK, position {p['latitude']:.4f},{p['longitude']:.4f} fmt={p.get('format')}"
                    else:
                        verdict = f"-> parsed OK but NO position (format={p.get('format')})"
                except Exception as e:
                    verdict = f"-> aprslib FAILED: {e}"
            if shown < args.limit:
                shown += 1
                tag = "SRC " if is_src else "mention"
                print(f"[{tag}] {ts}\n        {pkt}\n        {verdict}\n")
        last_id = entries[-1][0]
        if isinstance(last_id, bytes):
            last_id = last_id.decode()
        cursor = "(" + last_id
        if len(entries) < 5000:
            break

    print(f"Matches: {total} total, {as_source} with {needle} as SOURCE call, "
          f"{with_pos} carrying a parsable position.")
    if total and not as_source:
        print("NOTE: the callsign only appears in paths/objects/comments -- "
              "it may be beaconed as an APRS *object* by another station, "
              "in which case filter on the object name, not the source call.")

if __name__ == "__main__":
    main()
