#!/usr/bin/env python3
"""
parsefail_probe.py - Show WHAT is failing to parse in the Valkey `aprsis` stream.

tracker_health.py counts a "PARSEFAIL" whenever aprslib.parse() raises. This
probe reproduces those failures and categorizes them, so you can tell whether
they're harmless (status packets, messages, telemetry) or a real position-decode
problem (e.g. Mic-E frames aprslib chokes on).

For each source (optionally limited with --calls) it reports:
  * how many packets failed, grouped by APRS data-type and by aprslib's error
  * a few verbatim sample packets per category
  * the exact aprslib exception text

Requires:  pip install redis aprslib

Usage:
  python3 parsefail_probe.py --calls W1CMS-7,W1CMS-8,W1CMS-9,W1CMS-10,W1CMS-11 \
      --start 2026-07-10T00:00:00 --end 2026-07-11T23:59:59
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
try:
    import aprslib
except ImportError:
    sys.exit("aprslib is required:  pip install aprslib")

FRAC = re.compile(r"\.(\d+)")

# APRS data type identifier (first char of the information field) -> label
DTI = {
    "!": "position (no ts, no msg)", "=": "position (no ts, msg)",
    "/": "position (ts, no msg)", "@": "position (ts, msg)",
    ">": "status", "<": "station-capabilities", ":": "message",
    ";": "object", ")": "item", "T": "telemetry (T#)", "$": "raw-GPS/NMEA",
    "`": "mic-e", "'": "mic-e (old)", "_": "weather", "#": "weather-peet",
    "%": "agrelo", "&": "reserved", "*": "complete-weather", "}": "third-party",
    "!!": "ultimeter",
}

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

def dti_of(info):
    if not info:
        return "empty"
    if info[:2] == "!!":
        return DTI["!!"]
    return DTI.get(info[0], f"other '{info[0]}'")

def main():
    ap = argparse.ArgumentParser(description="Categorize aprslib parse failures")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--db", type=int, default=0)
    ap.add_argument("--stream", default="aprsis")
    ap.add_argument("--calls", default="", help="restrict to these sources")
    ap.add_argument("--start", type=parse_arg_time, default=None)
    ap.add_argument("--end", type=parse_arg_time, default=None)
    ap.add_argument("--samples", type=int, default=3, help="sample packets per category")
    ap.add_argument("--show-mice", action="store_true",
                    help="print every failing mic-e packet (for deeper debugging)")
    args = ap.parse_args()

    end = args.end or datetime.now(timezone.utc)
    start = args.start or (end - timedelta(days=2))
    want = {c.strip().upper() for c in args.calls.split(",") if c.strip()}

    r = redis.Redis(host=args.host, port=args.port, db=args.db)
    print(f"aprslib version: {getattr(aprslib, '__version__', 'unknown')}\n")

    total = fails = 0
    by_dti = Counter()                       # failing data-type -> count
    by_err = Counter()                       # aprslib error -> count
    by_call_dti = defaultdict(Counter)       # call -> {dti: count}
    samples = defaultdict(list)              # dti -> [(err, raw), ...]
    mice_fail = []

    for raw in iter_stream(r, args.stream, start, end):
        if ">" not in raw or ":" not in raw:
            continue
        src = raw.split(">", 1)[0].upper()
        if want and src not in want:
            continue
        total += 1
        try:
            aprslib.parse(raw)
            continue
        except Exception as e:
            fails += 1
            info = raw.split(":", 1)[1]
            dti = dti_of(info)
            try:
                msg = str(e).splitlines()[0][:80]
            except Exception:
                msg = ""
            by_dti[dti] += 1
            by_err[f"{e.__class__.__name__}: {msg}"] += 1
            by_call_dti[src][dti] += 1
            if len(samples[dti]) < args.samples:
                samples[dti].append((e.__class__.__name__, raw.strip()))
            if dti.startswith("mic-e"):
                mice_fail.append(raw.strip())

    print(f"Scanned {total} packets"
          + (f" from {len(want)} tracker(s)" if want else "")
          + f", window {start:%Y-%m-%d %H:%M}-{end:%Y-%m-%d %H:%M}z")
    print(f"Parse failures: {fails} ({100*fails/total:.1f}% of scanned)\n" if total else "\n")

    print("=== failures by APRS data type ===")
    for dti, n in by_dti.most_common():
        print(f"  {n:>6}  {dti}")

    print("\n=== failures by aprslib error (top 10) ===")
    for err, n in by_err.most_common(10):
        print(f"  {n:>6}  {err}")

    print("\n=== per-tracker breakdown ===")
    for call in sorted(by_call_dti):
        parts = ", ".join(f"{dti}:{n}" for dti, n in by_call_dti[call].most_common())
        print(f"  {call:10} {parts}")

    print("\n=== sample failing packets per type ===")
    for dti, items in samples.items():
        print(f"\n[{dti}]")
        for err, raw in items:
            print(f"  ({err}) {raw[:160]}")

    if args.show_mice and mice_fail:
        print(f"\n=== all {len(mice_fail)} failing mic-e packets ===")
        for raw in mice_fail:
            print(f"  {raw}")

    # quick verdict
    pos_types = sum(n for d, n in by_dti.items() if d.startswith("position") or d.startswith("mic-e"))
    nonpos = fails - pos_types
    print("\n--- verdict ---")
    print(f"  {nonpos} of {fails} failures are non-position packets "
          f"(status/message/telemetry) -> harmless, just not positions.")
    print(f"  {pos_types} are position/mic-e packets -> these are the ones that")
    print(f"  would actually cost you track points; investigate if non-zero.")

if __name__ == "__main__":
    main()
