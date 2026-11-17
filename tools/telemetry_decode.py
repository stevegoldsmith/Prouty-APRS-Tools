#!/usr/bin/env python3
"""
telemetry_decode.py - Decode APRS T# telemetry from a Valkey stream, using each
tracker's own PARM / UNIT / EQNS definitions to convert raw 0-255 counts into
engineering units (volts, degrees, ...).

APRS telemetry works like this:
  * data packet:  T#sss,a1,a2,a3,a4,a5,bbbbbbbb   (5 analog counts 0-255 + 8 bits)
  * the meaning/scale is described by messages the station sends about itself:
      :OWNER    :PARM.name1,...        (channel names)
      :OWNER    :UNIT.unit1,...        (channel units)
      :OWNER    :EQNS.a1,b1,c1,...     (per channel: value = a*raw^2 + b*raw + c)
This tool auto-discovers those definitions in the stream and applies them. If a
tracker never sent EQNS, it falls back to (in order): a --eqns you supply, an
empirical scale derived from a voltage like "13.3V" in the tracker's own position
comments, or raw counts (clearly labelled).

Requires:  pip install redis   (aprslib NOT needed; parsing is done here)

Usage:
  python3 telemetry_decode.py \
    --calls W1CMS-7,W1CMS-8,W1CMS-9,W1CMS-10,W1CMS-11 \
    --start 2026-07-10T00:00:00 --end 2026-07-11T23:59:59 \
    --min-voltage 11.5 --csv
"""

import argparse
import csv
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import redis
except ImportError:
    sys.exit("redis-py is required:  pip install redis")

FRAC = re.compile(r"\.(\d+)")
T_RE = re.compile(r"^T#(\w{1,3}),(.+)$")
VOLT_IN_COMMENT = re.compile(r"(\d{1,2}\.\d{1,2})\s*V", re.I)
VOLT_NAME = re.compile(r"volt|vbat|batt|vin|pwr|power|\bbat\b|^u$", re.I)
VOLT_UNIT = re.compile(r"^v(olt|olts|dc)?$", re.I)

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
            if not pkt:
                continue
            tsf = g("timestamp")
            if tsf:
                ts = parse_iso(tsf)
            else:
                ids = entry_id.decode() if isinstance(entry_id, bytes) else entry_id
                ts = datetime.fromtimestamp(int(ids.split("-")[0]) / 1000, tz=timezone.utc)
            yield ts, pkt
        last_id = entries[-1][0]
        if isinstance(last_id, bytes):
            last_id = last_id.decode()
        cursor = "(" + last_id
        if len(entries) < batch:
            break

def src_and_info(raw):
    if ">" not in raw or ":" not in raw:
        return None, None
    return raw.split(">", 1)[0].upper(), raw.split(":", 1)[1]

def parse_t(info):
    m = T_RE.match(info)
    if not m:
        return None
    fields = m.group(2).split(",")
    analog = []
    for f in fields[:5]:
        f = f.strip()
        try:
            analog.append(int(f))
        except ValueError:
            analog.append(None)
    bits = re.sub(r"[^01]", "", fields[5])[:8] if len(fields) >= 6 else ""
    return {"seq": m.group(1), "analog": analog, "bits": bits}

def parse_msg(info):
    """:ADDRESSEE:text  -> (addressee, text) or None"""
    if not info.startswith(":") or len(info) < 11 or info[10] != ":":
        return None
    return info[1:10].strip().upper(), info[11:].rstrip()

def parse_defs(text):
    """Return ('PARM'|'UNIT'|'EQNS'|'BITS', payload) or None."""
    for key in ("PARM.", "UNIT.", "EQNS.", "BITS."):
        if text.upper().startswith(key):
            return key[:-1], text[len(key):]
    return None

def eqns_triples(payload):
    nums = []
    for x in payload.split(","):
        x = x.strip()
        if x == "":
            continue
        try:
            nums.append(float(x))
        except ValueError:
            nums.append(0.0)
    return [tuple(nums[i:i + 3]) for i in range(0, (len(nums) // 3) * 3, 3)]

def apply_eqns(raw, triple):
    if raw is None or triple is None:
        return None
    a, b, c = (list(triple) + [0, 0, 0])[:3]
    return a * raw * raw + b * raw + c

def main():
    ap = argparse.ArgumentParser(description="Decode APRS telemetry with per-station EQNS")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--db", type=int, default=0)
    ap.add_argument("--stream", default="aprsis")
    ap.add_argument("--calls", default="", help="restrict to these telemetry sources")
    ap.add_argument("--roster", default=None,
                    help="CSV (callsign,tactical) for this day; supplies the source list when "
                         "--calls is omitted, and labels output as 'Tactical (CALLSIGN)'.")
    ap.add_argument("--start", type=parse_arg_time, default=None)
    ap.add_argument("--end", type=parse_arg_time, default=None)
    ap.add_argument("--min-voltage", type=float, default=None, help="warn if voltage drops below this")
    ap.add_argument("--eqns", default="", help="fallback EQNS 'a1,b1,c1,...' when a tracker sent none")
    ap.add_argument("--volt-channel", type=int, default=None, help="force analog channel N (1-5) as voltage")
    ap.add_argument("--csv", action="store_true", help="write decoded time series CSV")
    ap.add_argument("--out-dir", default="telemetry")
    args = ap.parse_args()

    end = args.end or datetime.now(timezone.utc)
    start = args.start or (end - timedelta(days=2))
    roster = {}
    if args.roster:
        import csv as _csv
        with open(args.roster, newline="") as f:
            for row in _csv.DictReader(f):
                cs = (row.get("callsign") or "").strip().upper()
                tac = (row.get("tactical") or "").strip()
                if cs and tac:
                    roster[cs] = tac
    want = {c.strip().upper() for c in args.calls.split(",") if c.strip()}
    if not want and roster:
        want = set(roster.keys())
    def label(cs):
        t = roster.get(cs.upper())
        return f"{t} ({cs})" if t else cs
    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    fallback_eqns = eqns_triples(args.eqns) if args.eqns else None

    tele = {}     # call -> list of {ts, seq, analog, bits}
    defs = {}     # call -> {'PARM':[...], 'UNIT':[...], 'EQNS':[triples]}
    comment_v = {}  # call -> list of (ts, volts) from position comments

    r = redis.Redis(host=args.host, port=args.port, db=args.db)
    scanned = 0
    for ts, raw in iter_stream(r, args.stream, start, end):
        scanned += 1
        src, info = src_and_info(raw)
        if not src or not info:
            continue

        # telemetry definitions (attributed to the addressee = telemetry owner)
        msg = parse_msg(info)
        if msg:
            owner, text = msg
            d = parse_defs(text)
            if d:
                if want and owner not in want:
                    pass
                else:
                    kind, payload = d
                    slot = defs.setdefault(owner, {})
                    slot[kind] = eqns_triples(payload) if kind == "EQNS" else \
                        [p.strip() for p in payload.split(",")]
            continue

        if want and src not in want:
            continue

        # T# data
        t = parse_t(info)
        if t:
            tele.setdefault(src, []).append({"ts": ts, **t})
            continue

        # voltage in a position comment, for empirical fallback calibration
        vm = VOLT_IN_COMMENT.search(info)
        if vm:
            try:
                comment_v.setdefault(src, []).append((ts, float(vm.group(1))))
            except ValueError:
                pass

    calls = sorted(want) if want else sorted(tele)
    print(f"Scanned {scanned} entries, window {start:%Y-%m-%d %H:%M}-{end:%Y-%m-%d %H:%M}z\n")

    rows = []      # for CSV
    for call in calls:
        samples = tele.get(call, [])
        if not samples:
            print(f"{label(call)}: no T# telemetry in window\n")
            continue
        samples.sort(key=lambda s: s["ts"])
        d = defs.get(call, {})
        parm = d.get("PARM")
        unit = d.get("UNIT")
        eqns = d.get("EQNS")

        # decide EQNS source
        eqns_src = "stream"
        if not eqns and fallback_eqns:
            eqns, eqns_src = fallback_eqns, "--eqns"

        # pick voltage channel
        vch = None
        if args.volt_channel:
            vch = args.volt_channel - 1
        elif parm:
            for i, name in enumerate(parm[:5]):
                if VOLT_NAME.search(name):
                    vch = i; break
        if vch is None and unit:
            for i, u in enumerate(unit[:5]):
                if VOLT_UNIT.search(u):
                    vch = i; break
        if vch is None:
            vch = 0   # Voyager default: channel 1 is input voltage

        # empirical calibration if we still have no EQNS for the voltage channel
        emp_scale = None
        if (not eqns or vch >= len(eqns) or eqns[vch] == (0, 0, 0)) and comment_v.get(call):
            # match each comment voltage to the nearest T# sample's raw value on vch
            pairs = []
            for cts, cv in comment_v[call]:
                near = min(samples, key=lambda s: abs((s["ts"] - cts).total_seconds()))
                if abs((near["ts"] - cts).total_seconds()) <= 120 and near["analog"][vch]:
                    pairs.append((near["analog"][vch], cv))
            if pairs:
                # least-squares scale through origin: v = k*raw
                num = sum(raw * v for raw, v in pairs)
                den = sum(raw * raw for raw, v in pairs)
                if den:
                    emp_scale = num / den
                    eqns_src = f"empirical ({len(pairs)} comment-V matches)"

        def decode(raw_val):
            if emp_scale is not None:
                return emp_scale * raw_val if raw_val is not None else None
            if eqns and vch < len(eqns):
                return apply_eqns(raw_val, eqns[vch])
            return raw_val   # raw counts, unscaled

        vch_name = (parm[vch] if parm and vch < len(parm) else f"ch{vch+1}")
        vch_unit = (unit[vch] if unit and vch < len(unit) else ("V" if emp_scale or eqns else "raw"))

        vals = [decode(s["analog"][vch]) for s in samples if s["analog"][vch] is not None]
        print(f"=== {label(call)} ===")
        print(f"  T# samples: {len(samples)}   "
              f"first {samples[0]['ts']:%m-%d %H:%M}z  last {samples[-1]['ts']:%m-%d %H:%M}z")
        if parm:
            print(f"  PARM: {','.join(parm[:5])}")
        if unit:
            print(f"  UNIT: {','.join(unit[:5])}")
        if eqns:
            print(f"  EQNS: {eqns}   (source: {eqns_src})")
        elif emp_scale is not None:
            print(f"  EQNS: none in stream; voltage scale ~{emp_scale:.4f} V/count ({eqns_src})")
        else:
            print(f"  EQNS: none found -> channel values shown as RAW counts (uncalibrated)")
        if not parm and not unit and not eqns and emp_scale is None:
            print(f"  (no telemetry definitions seen for {call}; run longer window to catch PARM/EQNS)")

        if vals:
            lo, hi, last = min(vals), max(vals), vals[-1]
            trend = "steady" if abs(vals[-1] - vals[0]) < 0.3 else \
                    ("falling" if vals[-1] < vals[0] else "rising")
            unit_lbl = vch_unit if (eqns or emp_scale) else "counts"
            print(f"  Voltage channel: ch{vch+1} '{vch_name}'  "
                  f"min {lo:.2f} / max {hi:.2f} / last {last:.2f} {unit_lbl}  ({trend})")
            if args.min_voltage is not None and (eqns or emp_scale) and lo < args.min_voltage:
                # find when it first dropped below
                first_low = next((s["ts"] for s in samples
                                  if s["analog"][vch] is not None
                                  and decode(s["analog"][vch]) < args.min_voltage), None)
                when = f" first at {first_low:%m-%d %H:%M}z" if first_low else ""
                print(f"  ** LOWVOLT: dropped to {lo:.2f} {unit_lbl} (< {args.min_voltage}){when}")
        print()

        if args.csv:
            for s in samples:
                dec = [decode(s["analog"][vch]) if i == vch else s["analog"][i]
                       for i in range(5)]
                rows.append([call, s["ts"].isoformat(), s["seq"]] +
                            [s["analog"][i] for i in range(5)] +
                            [f"{decode(s['analog'][vch]):.3f}" if s["analog"][vch] is not None else "",
                             vch + 1, vch_name, s["bits"]])

    if args.csv and rows:
        path = out_dir / f"telemetry_{start:%Y%m%d}_{end:%Y%m%d}.csv"
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["tracker", "utc", "seq", "raw1", "raw2", "raw3", "raw4", "raw5",
                        "volt_decoded", "volt_channel", "volt_name", "bits"])
            w.writerows(rows)
        print(f"Decoded time series -> {path}")

if __name__ == "__main__":
    main()
