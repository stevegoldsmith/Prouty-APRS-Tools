#!/usr/bin/env python3
"""
tracker_health.py - Find anomalies in mobile APRS trackers from a Valkey stream.

Scans the `aprsis` stream over a window, groups position packets by source
callsign, and checks each mobile tracker for signs of trouble:

  NOFIX       position at/near 0,0 (null island) -- invalid GPS fix
  STALEDEFAULT tracker beaconed many times but its position never changed
              (dead GPS sending a default/last-known coordinate)
  FROZEN      position stuck for a stretch while it reported speed > 0
              (GPS lost lock but tracker kept moving)
  JUMP        implied speed between two fixes is impossible (bad fix / clock)
  DROPOUT     gap between beacons longer than --gap-min (tracker off / dead spot)
  SPARSE      very few beacons in the whole window
  LOWVOLT     voltage in the beacon comment fell below --min-voltage
  PARSEFAIL   packets from this source that would not parse (malformed output)

By default it AUTO-DISCOVERS mobile stations (those that actually moved) and
skips fixed infrastructure. Pass --calls to analyze an explicit tracker list
instead -- important for catching a tracker whose GPS died so completely it
never moved (auto-discovery would treat it as fixed and skip it).

Outputs a ranked console report (worst first), a per-tracker CSV, and an
event CSV. Add --kmz to also drop an anomalies KMZ marking JUMP and FROZEN
locations for visual inspection in Google Earth.

Requires:  pip install redis aprslib   (pillow only needed with --kmz)

Usage:
  python3 tracker_health.py --start 2026-07-10T00:00:00 --end 2026-07-11T23:59:59
  python3 tracker_health.py --calls W1CMS-1,W1CMS-9,W1HS-9,W1HS-13,W1TGA-9 --kmz
"""

import argparse
import csv
import math
import re
import sys
import zipfile
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

try:
    import redis
except ImportError:
    sys.exit("redis-py is required:  pip install redis")
try:
    import aprslib
except ImportError:
    sys.exit("aprslib is required:  pip install aprslib")
try:
    from aprslib.exceptions import UnknownFormat as _UnknownFormat
except Exception:
    class _UnknownFormat(Exception):
        pass

FRAC = re.compile(r"\.(\d+)")
VOLT_RE = re.compile(r"(?:U=)?(\d{1,2}\.\d{1,2})\s*V", re.I)
# Explicit GPS-loss status text (e.g. Microsat Voyager sends "GPS lock lost").
# Case-insensitive; override with --gps-lost-pattern for other firmware wording.
DEFAULT_GPS_LOST = r"gps\s*(?:lock\s*)?lost|lost\s*gps|no\s*gps|gps\s*(?:invalid|no\s*fix)"
# Recovery status (Microsat Voyager sends "GPS Locked"). Used to bound outages
# precisely; must NOT match the loss string "GPS Lock Lost".
DEFAULT_GPS_OK = r"gps\s*locked|gps\s*(?:ok|fix|acquired|3d)"

# --------------------------------------------------------------- utilities ---

def parse_iso(ts):
    ts = ts.strip().replace("Z", "+00:00")
    ts = FRAC.sub(lambda m: "." + m.group(1)[:6], ts, count=1)
    dt = datetime.fromisoformat(ts)
    return (dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None
            else dt.astimezone(timezone.utc))

def parse_arg_time(s):
    dt = datetime.fromisoformat(s)
    return (dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None
            else dt.astimezone(timezone.utc))

def ms(dt):
    return int(dt.timestamp() * 1000)

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))

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

def source_of(raw):
    return raw.split(">", 1)[0].upper() if ">" in raw else None

# ---------------------------------------------------------------- detectors ---

def detect_jumps(pts, max_kmh, jump_km, min_dt=2.0):
    jumps = []
    for i in range(len(pts) - 1):
        p, q = pts[i], pts[i + 1]
        dt = (q["ts"] - p["ts"]).total_seconds()
        # Skip steps shorter than min_dt: these are the same beacon heard twice
        # (two igate paths), or near-simultaneous duplicates in the feed. A real
        # vehicle does not produce two fixes >jump_km apart within a couple of
        # seconds, and tiny dt is what manufactures the nonsense 10000+ km/h
        # "teleports". Genuine bad fixes (a real spike over a normal interval)
        # still have a normal dt and are kept.
        if dt < min_dt:
            continue
        d = haversine_km(p["lat"], p["lon"], q["lat"], q["lon"])
        if d >= jump_km and d / (dt / 3600) > max_kmh:
            spike = False
            if i + 2 < len(pts):
                back = haversine_km(q["lat"], q["lon"], pts[i + 2]["lat"], pts[i + 2]["lon"])
                bridge = haversine_km(p["lat"], p["lon"], pts[i + 2]["lat"], pts[i + 2]["lon"])
                spike = back >= jump_km and bridge < jump_km
            jumps.append({"t0": p["ts"], "t1": q["ts"], "km": round(d, 2),
                          "dt": int(dt), "kmh": int(d / (dt / 3600)),
                          "lat": q["lat"], "lon": q["lon"], "spike": spike})
    return jumps

def detect_frozen(pts, min_min, min_count, moving_kmh):
    episodes = []
    i, n = 0, len(pts)
    while i < n:
        j = i
        key = (round(pts[i]["lat"], 5), round(pts[i]["lon"], 5))
        while j + 1 < n and (round(pts[j + 1]["lat"], 5), round(pts[j + 1]["lon"], 5)) == key:
            j += 1
        run = pts[i:j + 1]
        if len(run) >= min_count:
            dur = (run[-1]["ts"] - run[0]["ts"]).total_seconds() / 60
            spds = [p["speed"] for p in run if p["speed"] is not None]
            maxspd = max(spds) if spds else 0
            if dur >= min_min and maxspd > moving_kmh:
                episodes.append({"lat": key[0], "lon": key[1], "min": round(dur, 1),
                                 "count": len(run), "maxspeed": round(maxspd),
                                 "t0": run[0]["ts"], "t1": run[-1]["ts"]})
        i = j + 1
    return episodes

def compute_outages(lost, oks, pts):
    """Pair each GPS-lost status with the earliest recovery signal after it:
    a 'GPS Locked' status if present, else the next valid position fix.
    Repeated lost statuses before a recovery collapse into one outage."""
    pos_ts = [p["ts"] for p in pts]
    lost = sorted(lost); oks = sorted(oks)
    out = []
    i = 0
    while i < len(lost):
        startt = lost[i]
        rec_ok = next((t for t in oks if t > startt), None)
        rec_pos = next((t for t in pos_ts if t > startt), None)
        cands = [t for t in (rec_ok, rec_pos) if t is not None]
        rec = min(cands) if cands else None
        rec_src = ("status" if rec is not None and rec == rec_ok
                   else ("fix" if rec is not None else None))
        j = i + 1
        while j < len(lost) and (rec is None or lost[j] < rec):
            j += 1
        prev = [p for p in pts if p["ts"] <= startt]
        loc = (prev[-1]["lat"], prev[-1]["lon"]) if prev else None
        dur = (rec - startt).total_seconds() / 60 if rec else None
        out.append({"start": startt, "rec": rec, "count": j - i,
                    "min": round(dur, 1) if dur is not None else None,
                    "loc": loc, "rec_src": rec_src})
        i = j
    return out

def analyze_tracker(call, pts, raws, parsefail, gpslost, gpsok, args):
    pts.sort(key=lambda p: p["ts"])
    n = len(pts)
    first, last = pts[0]["ts"], pts[-1]["ts"]
    active_min = (last - first).total_seconds() / 60

    # total path distance & bounding-box size
    dist = 0.0
    for a, b in zip(pts, pts[1:]):
        dist += haversine_km(a["lat"], a["lon"], b["lat"], b["lon"])
    lats = [p["lat"] for p in pts]; lons = [p["lon"] for p in pts]
    bbox = haversine_km(min(lats), min(lons), max(lats), max(lons))

    uniq = {(round(p["lat"], 5), round(p["lon"], 5)) for p in pts}
    nullc = sum(1 for p in pts if abs(p["lat"]) < 0.05 and abs(p["lon"]) < 0.05)

    # beacon gaps
    gaps = []
    for a, b in zip(pts, pts[1:]):
        g = (b["ts"] - a["ts"]).total_seconds()
        if g > args.gap_min * 60:
            gaps.append({"t0": a["ts"], "t1": b["ts"], "s": int(g)})
    max_gap = max((g["s"] for g in gaps), default=0)

    jumps = detect_jumps(pts, args.max_speed, args.jump_km, args.min_jump_dt)
    frozen = detect_frozen(pts, args.frozen_min, args.frozen_count, args.moving_kmh)

    volts = [float(m) for r in raws for m in VOLT_RE.findall(r)]
    min_volt = min(volts) if volts else None

    # explicit GPS-lost status episodes, bounded by 'GPS Locked' or next fix.
    outages = compute_outages(gpslost, gpsok, pts)

    flags = []
    if outages:
        flags.append("GPSLOST")
    if nullc:
        flags.append("NOFIX")
    if len(uniq) == 1 and n >= max(args.sparse_pkts, 3):
        flags.append("STALEDEFAULT")
    if frozen:
        flags.append("FROZEN")
    if jumps:
        flags.append("JUMP")
    if gaps:
        flags.append("DROPOUT")
    if n < args.sparse_pkts:
        flags.append("SPARSE")
    if args.min_voltage and min_volt is not None and min_volt < args.min_voltage:
        flags.append("LOWVOLT")
    if parsefail:
        flags.append("PARSEFAIL")

    severity = (6 * len(outages) + 5 * len(jumps) + 4 * len(frozen) + 5 * nullc
                + (3 if "STALEDEFAULT" in flags else 0) + 2 * len(gaps)
                + (2 if "SPARSE" in flags else 0)
                + (2 if "LOWVOLT" in flags else 0) + parsefail)

    return {
        "call": call, "pkts": n, "parsefail": parsefail,
        "first": first, "last": last, "active_min": round(active_min, 1),
        "dist_km": round(dist, 2), "bbox_km": round(bbox, 2),
        "uniq": len(uniq), "nullc": nullc,
        "gaps": gaps, "max_gap_s": max_gap,
        "jumps": jumps, "frozen": frozen, "outages": outages,
        "min_volt": min_volt, "flags": flags, "severity": severity,
    }

# ---------------------------------------------------------------- kmz out ---

def write_anomaly_kmz(path, results):
    try:
        from PIL import Image, ImageDraw
    except ImportError:
        print("  (--kmz needs Pillow; skipping KMZ)")
        return
    def marker(color):
        img = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
        d = ImageDraw.Draw(img)
        d.ellipse([10, 10, 54, 54], fill=color, outline=(0, 0, 0, 200), width=3)
        b = BytesIO(); img.save(b, "PNG"); return b.getvalue()
    jump_png = marker((255, 0, 0, 255))       # red = jump/spike
    frozen_png = marker((255, 200, 0, 255))   # amber = frozen
    lost_png = marker((160, 0, 200, 255))     # purple = GPS lock lost

    folders = []
    for res in results:
        if not res["jumps"] and not res["frozen"] and not res.get("outages"):
            continue
        pms = []
        for o in res.get("outages", []):
            if not o["loc"]:
                continue   # status-only, no last-known position to place
            dur = f"{o['min']}min" if o["min"] is not None else "no recovery"
            pms.append(
                f'<Placemark><styleUrl>#lost</styleUrl>'
                f'<name>{res["call"]} GPS lock lost</name>'
                f'<description><![CDATA[went dark {o["start"]:%Y-%m-%d %H:%M:%S}z<br/>'
                f'reacquired: {dur}]]></description>'
                f'<Point><coordinates>{o["loc"][1]:.5f},{o["loc"][0]:.5f},0'
                f'</coordinates></Point></Placemark>')
        for j in res["jumps"]:
            pms.append(
                f'<Placemark><styleUrl>#jump</styleUrl>'
                f'<name>{res["call"]} {"spike" if j["spike"] else "jump"}</name>'
                f'<description><![CDATA[{j["km"]} km in {j["dt"]}s = {j["kmh"]} km/h<br/>'
                f'{j["t1"]:%Y-%m-%d %H:%M:%S}z]]></description>'
                f'<Point><coordinates>{j["lon"]:.5f},{j["lat"]:.5f},0</coordinates></Point></Placemark>')
        for fz in res["frozen"]:
            pms.append(
                f'<Placemark><styleUrl>#frozen</styleUrl>'
                f'<name>{res["call"]} frozen {fz["min"]}min</name>'
                f'<description><![CDATA[stuck {fz["count"]} fixes / {fz["min"]} min '
                f'while reporting up to {fz["maxspeed"]} km/h<br/>'
                f'{fz["t0"]:%H:%M:%S}-{fz["t1"]:%H:%M:%S}z]]></description>'
                f'<Point><coordinates>{fz["lon"]:.5f},{fz["lat"]:.5f},0</coordinates></Point></Placemark>')
        folders.append(f'<Folder><name>{res["call"]} ({len(pms)})</name>\n'
                       + "\n".join(pms) + "\n</Folder>")
    if not folders:
        print("  no mappable anomalies (JUMP/FROZEN) to put in KMZ")
        return
    kml = f"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2"><Document><name>Tracker anomalies</name>
<Style id="jump"><IconStyle><scale>1.2</scale><Icon><href>files/jump.png</href></Icon></IconStyle></Style>
<Style id="frozen"><IconStyle><scale>1.2</scale><Icon><href>files/frozen.png</href></Icon></IconStyle></Style>
<Style id="lost"><IconStyle><scale>1.3</scale><Icon><href>files/lost.png</href></Icon></IconStyle></Style>
{chr(10).join(folders)}
</Document></kml>"""
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("anomalies.kml", kml)
        z.writestr("files/jump.png", jump_png)
        z.writestr("files/frozen.png", frozen_png)
        z.writestr("files/lost.png", lost_png)
    print(f"  anomaly KMZ -> {path}")

# ------------------------------------------------------------------- main ---

def main():
    ap = argparse.ArgumentParser(description="Mobile tracker anomaly report from Valkey")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--db", type=int, default=0)
    ap.add_argument("--stream", default="aprsis")
    ap.add_argument("--calls", default="", help="explicit tracker list; omit to auto-discover mobiles")
    ap.add_argument("--roster", default=None,
                    help="CSV (callsign,tactical) for this day; supplies the tracker list when "
                         "--calls is omitted, and labels output as 'Tactical (CALLSIGN)'.")
    ap.add_argument("--exclude", default="", help="callsigns to always skip (e.g. known digis)")
    ap.add_argument("--start", type=parse_arg_time, default=None)
    ap.add_argument("--end", type=parse_arg_time, default=None)
    ap.add_argument("--out-dir", default="tracker_health")
    ap.add_argument("--kmz", action="store_true", help="also write an anomalies KMZ")
    ap.add_argument("--gap-min", type=float, default=12, help="beacon gap (min) = DROPOUT")
    ap.add_argument("--max-speed", type=float, default=160, help="km/h above which a step = JUMP")
    ap.add_argument("--min-jump-dt", type=float, default=2.0,
                    help="ignore steps shorter than this many seconds (duplicate/near-simultaneous packets)")
    ap.add_argument("--jump-km", type=float, default=0.5, help="min step distance to consider a JUMP")
    ap.add_argument("--frozen-min", type=float, default=5, help="min minutes stuck to consider FROZEN")
    ap.add_argument("--frozen-count", type=int, default=3, help="min identical fixes for FROZEN")
    ap.add_argument("--moving-kmh", type=float, default=5, help="speed during a frozen run to flag it")
    ap.add_argument("--mobile-km", type=float, default=1.0, help="auto-discovery: min bbox to be 'mobile'")
    ap.add_argument("--sparse-pkts", type=int, default=5, help="fewer position packets = SPARSE")
    ap.add_argument("--min-voltage", type=float, default=None, help="flag LOWVOLT below this (e.g. 11.5)")
    ap.add_argument("--gps-lost-pattern", default=DEFAULT_GPS_LOST,
                    help="regex (case-insensitive) for GPS-loss status text; "
                         "default matches Microsat Voyager 'GPS Lock Lost' and variants")
    ap.add_argument("--gps-ok-pattern", default=DEFAULT_GPS_OK,
                    help="regex for GPS-recovery status text (Voyager 'GPS Locked'); "
                         "used to measure outage length precisely")
    ap.add_argument("--no-gps-status", action="store_true",
                    help="disable GPS-lost status-text detection")
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
        want = set(roster.keys())            # roster supplies the tracker list
    def label(cs):
        t = roster.get(cs.upper())
        return f"{t} ({cs})" if t else cs
    exclude = {c.strip().upper() for c in args.exclude.split(",") if c.strip()}
    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)

    pts_by = {}      # call -> list of position dicts
    raw_by = {}      # call -> list of raw payloads (for voltage)
    fail_by = {}     # call -> parse-failure count
    lost_by = {}     # call -> timestamps of GPS-lost status text
    ok_by = {}       # call -> timestamps of GPS-recovery status text
    gps_re = None if args.no_gps_status else re.compile(args.gps_lost_pattern, re.I)
    ok_re = None if args.no_gps_status else re.compile(args.gps_ok_pattern, re.I)
    r = redis.Redis(host=args.host, port=args.port, db=args.db)

    scanned = 0
    for ts, raw in iter_stream(r, args.stream, start, end):
        scanned += 1
        src = source_of(raw)
        if not src or src in exclude:
            continue
        if want and src not in want:
            continue
        raw_by.setdefault(src, []).append(raw)
        if gps_re is not None and ":" in raw:
            info = raw.split(":", 1)[1]
            if gps_re.search(info):
                lost_by.setdefault(src, []).append(ts)
            elif ok_re.search(info):      # elif: a loss line never counts as recovery
                ok_by.setdefault(src, []).append(ts)
        try:
            p = aprslib.parse(raw)
        except _UnknownFormat:
            continue          # known type aprslib can't decode (e.g. T# telemetry) - not malformed
        except Exception:
            fail_by[src] = fail_by.get(src, 0) + 1
            continue
        if "latitude" not in p or "longitude" not in p:
            continue
        pts_by.setdefault(src, []).append({
            "ts": ts, "lat": p["latitude"], "lon": p["longitude"],
            "speed": p.get("speed"), "course": p.get("course"),
            "alt": p.get("altitude"),
        })

    # choose which stations to analyze
    results = []
    for call, pts in pts_by.items():
        if len(pts) < 2:
            continue
        if not want:   # auto-discovery: keep only stations that actually moved
            lats = [p["lat"] for p in pts]; lons = [p["lon"] for p in pts]
            if haversine_km(min(lats), min(lons), max(lats), max(lons)) < args.mobile_km:
                continue
        results.append(analyze_tracker(call, pts, raw_by.get(call, []),
                                       fail_by.get(call, 0), lost_by.get(call, []),
                                       ok_by.get(call, []), args))
    # trackers that ONLY produced parse failures / status / no position (explicit list)
    if want:
        for call in want:
            if call not in pts_by and (call in raw_by or call in lost_by):
                outs = compute_outages(lost_by.get(call, []), ok_by.get(call, []), [])
                fl = []
                if outs:
                    fl.append("GPSLOST")
                fl.append("NOPOS")
                results.append({"call": call, "pkts": 0, "parsefail": fail_by.get(call, 0),
                                "flags": fl, "severity": 4 + 6 * len(outs),
                                "first": None, "last": None,
                                "active_min": 0, "dist_km": 0, "bbox_km": 0, "uniq": 0,
                                "nullc": 0, "gaps": [], "max_gap_s": 0, "jumps": [],
                                "frozen": [], "outages": outs, "min_volt": None})

    results.sort(key=lambda r: -r["severity"])

    # ---- console report ----
    mode = f"explicit list ({len(want)})" if want else "auto-discovered mobiles"
    print(f"Scanned {scanned} entries.  Trackers: {mode}.  "
          f"Window {start:%Y-%m-%d %H:%M}-{end:%Y-%m-%d %H:%M}z\n")
    tac = bool(roster)
    namew = 28 if tac else 10
    print(f"{'tracker':{namew}} {'pkts':>5} {'active':>7} {'dist':>7} {'maxgap':>7}  flags")
    for r_ in results:
        gap_txt = f"{r_['max_gap_s']//60}m" if r_["max_gap_s"] else "-"
        flags = ",".join(r_["flags"]) if r_["flags"] else "ok"
        print(f"{label(r_['call']):{namew}} {r_['pkts']:>5} {r_['active_min']:>6.0f}m "
              f"{r_['dist_km']:>6.1f}k {gap_txt:>7}  {flags}")

    # ---- details for flagged trackers ----
    flagged = [r_ for r_ in results if r_["flags"] and r_["flags"] != ["ok"]]
    if flagged:
        print("\n--- details ---")
    for r_ in flagged:
        print(f"\n{label(r_['call'])}  [{','.join(r_['flags'])}]")
        for o in r_.get("outages", []):
            where = (f" near {o['loc'][0]:.5f},{o['loc'][1]:.5f}" if o["loc"] else "")
            via = {"status": " (via GPS Locked)", "fix": " (via next fix)"}.get(o.get("rec_src"), "")
            reps = f" ({o['count']} status beacons)" if o["count"] > 1 else ""
            if o["min"] is not None:
                dur = f"{o['min']}min" if o["min"] >= 1 else f"{int(o['min']*60)}s"
                print(f"  GPSLOST: GPS lock lost {o['start']:%H:%M:%S}z{where}, "
                      f"reacquired after {dur}{via}{reps}")
            else:
                print(f"  GPSLOST: GPS lock lost {o['start']:%H:%M:%S}z{where}, "
                      f"no recovery seen{reps}")
        if r_["nullc"]:
            print(f"  NOFIX: {r_['nullc']} packet(s) at ~0,0 (invalid GPS)")
        if "STALEDEFAULT" in r_["flags"]:
            print(f"  STALEDEFAULT: {r_['pkts']} packets but only 1 unique position "
                  f"(GPS never updated)")
        for fz in r_["frozen"]:
            print(f"  FROZEN: stuck {fz['count']} fixes / {fz['min']}min at "
                  f"{fz['lat']:.5f},{fz['lon']:.5f} while reporting up to "
                  f"{fz['maxspeed']} km/h ({fz['t0']:%H:%M}-{fz['t1']:%H:%M}z)")
        for j in r_["jumps"][:5]:
            kind = "spike" if j["spike"] else "jump"
            print(f"  JUMP ({kind}): {j['km']}km in {j['dt']}s = {j['kmh']}km/h "
                  f"at {j['t1']:%H:%M:%S}z")
        if len(r_["jumps"]) > 5:
            print(f"  ... and {len(r_['jumps'])-5} more jumps")
        for g in r_["gaps"][:3]:
            print(f"  DROPOUT: {g['s']//60}m{g['s']%60:02d}s silent "
                  f"{g['t0']:%H:%M}-{g['t1']:%H:%M}z")
        if len(r_["gaps"]) > 3:
            print(f"  ... and {len(r_['gaps'])-3} more dropouts")
        if "LOWVOLT" in r_["flags"]:
            print(f"  LOWVOLT: min {r_['min_volt']}V (< {args.min_voltage}V)")
        if r_["parsefail"]:
            print(f"  PARSEFAIL: {r_['parsefail']} malformed packet(s)")

    # ---- CSVs ----
    stamp = f"{start:%Y%m%d}_{end:%Y%m%d}"
    summary_csv = out_dir / f"tracker_health_{stamp}.csv"
    with open(summary_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["tracker", "tactical", "pkts", "parsefail", "active_min", "dist_km", "bbox_km",
                    "uniq_pos", "null_fixes", "gps_lost_events", "gps_lost_min",
                    "dropouts", "max_gap_s", "jumps", "frozen",
                    "min_volt", "severity", "flags"])
        for r_ in results:
            outs = r_.get("outages", [])
            lost_min = sum(o["min"] for o in outs if o["min"] is not None)
            w.writerow([r_["call"], roster.get(r_["call"].upper(), ""),
                        r_["pkts"], r_["parsefail"], r_.get("active_min", 0),
                        r_.get("dist_km", 0), r_.get("bbox_km", 0), r_.get("uniq", 0),
                        r_["nullc"], len(outs), round(lost_min, 1),
                        len(r_["gaps"]), r_["max_gap_s"], len(r_["jumps"]),
                        len(r_["frozen"]), r_["min_volt"] if r_["min_volt"] is not None else "",
                        r_["severity"], "|".join(r_["flags"])])

    events_csv = out_dir / f"tracker_events_{stamp}.csv"
    with open(events_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["tracker", "type", "start_utc", "end_utc", "detail", "lat", "lon"])
        for r_ in results:
            for o in r_.get("outages", []):
                lat, lon = (o["loc"] if o["loc"] else ("", ""))
                w.writerow([r_["call"], "gpslost", o["start"].isoformat(),
                            o["rec"].isoformat() if o["rec"] else "",
                            f"{o['min']}min" if o["min"] is not None else "no recovery",
                            lat, lon])
            for j in r_["jumps"]:
                w.writerow([r_["call"], "spike" if j["spike"] else "jump",
                            j["t0"].isoformat(), j["t1"].isoformat(),
                            f"{j['km']}km/{j['dt']}s={j['kmh']}kmh", j["lat"], j["lon"]])
            for fz in r_["frozen"]:
                w.writerow([r_["call"], "frozen", fz["t0"].isoformat(), fz["t1"].isoformat(),
                            f"{fz['count']}fixes/{fz['min']}min@{fz['maxspeed']}kmh",
                            fz["lat"], fz["lon"]])
            for g in r_["gaps"]:
                w.writerow([r_["call"], "dropout", g["t0"].isoformat(), g["t1"].isoformat(),
                            f"{g['s']}s", "", ""])

    print(f"\nSummary CSV -> {summary_csv}\nEvents  CSV -> {events_csv}")
    if args.kmz:
        write_anomaly_kmz(out_dir / f"tracker_anomalies_{stamp}.kmz", results)

if __name__ == "__main__":
    main()
