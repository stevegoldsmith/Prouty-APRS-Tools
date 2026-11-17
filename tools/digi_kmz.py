#!/usr/bin/env python3
"""
digi_kmz.py - Per-digi / per-igate traffic & coverage KMZ from a Valkey APRS stream.

For each target station, scans the `aprsis` stream over a time window and finds
every packet that station HANDLED off-air -- where it appears as a used
digipeater (X*) or as the RF igate that gated it (call after qAR/qAO). The
TRANSMITTING station's position is plotted for each such packet, so the dot
cloud maps that station's real reception footprint / traffic load.

Two things this version gets right that matter when your targets form an
infrastructure mesh:

  * FIELD vs INFRASTRUCTURE origin.  Your named stations constantly relay each
    other's beacons; those are real "handled" packets but they're infra chatter
    sitting on the other sites, not field coverage.  They go in a separate
    folder so each map's field traffic is clean.  Use --infra to name extra
    fixed stations (other digis/igates) that should also be treated as infra.

  * RF-heard only, for igates.  An igate is credited only for packets it heard
    on RF (qAR / qAO), NOT ones it received over the internet from another igate
    (qAS / qAC / qAI).  Add --include-net to count those too.

Within each folder, dots are colored DIRECT (green, station heard the vehicle
first-hand) vs RELAYED (gray, it handled the packet after another station
already repeated it).  The station's own beacon becomes a labeled home ring.
Icons are embedded so they render without network access.

Requires:  pip install redis aprslib pillow

Usage:
  python3 digi_kmz.py \
    --calls N1GMC-1,N1GMC-2,W1FN-9,W1FN-5,W1FN-1,W1FN-3,WX1NH-10 \
    --start 2026-07-10T00:00:00 --end 2026-07-11T23:59:59 \
    --out-dir ./prouty_digi_kmz
"""

import argparse
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
    from PIL import Image, ImageDraw
except ImportError:
    sys.exit("Pillow is required:  pip install pillow")

FRAC = re.compile(r"\.(\d+)")
Q_CODES = {"qAC", "qAI", "qAO", "qAo", "qAR", "qAr", "qAS", "qAX", "qAZ", "qBU"}
RF_Q = {"qAR", "qAO"}     # igate genuinely heard it on RF

# ------------------------------------------------------------ time / stream ---

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

def iter_stream(r, stream, start, end, batch=5000):
    cursor, hi = f"{ms(start)}-0", f"{ms(end)}-{2**64-1}"
    while True:
        entries = r.xrange(stream, min=cursor, max=hi, count=batch)
        if not entries:
            break
        for entry_id, fields in entries:
            keys_are_bytes = isinstance(next(iter(fields)), bytes)
            def g(k):
                v = fields.get(k.encode()) if keys_are_bytes else fields.get(k)
                return v.decode() if isinstance(v, bytes) else v
            pkt = g("packet")
            if not pkt:
                continue
            ts_field = g("timestamp")
            if ts_field:
                ts = parse_iso(ts_field)
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

# ----------------------------------------------------------- path analysis ---

def parse_header(raw):
    header = raw.split(":", 1)[0]
    if ">" not in header:
        return None, None, []
    src, rest = header.split(">", 1)
    parts = rest.split(",")
    return src, parts[0], parts[1:]

def split_q(path):
    for i, e in enumerate(path):
        if e in Q_CODES:
            igate = path[i + 1].rstrip("*") if i + 1 < len(path) else None
            return path[:i], igate, e
    return path, None, None

def used_digis(digipath):
    last_used = -1
    for i, e in enumerate(digipath):
        if e.endswith("*"):
            last_used = i
    return [e.rstrip("*") for i, e in enumerate(digipath) if i <= last_used]

def analyze(raw, target, include_net):
    """
    Return dict describing target's involvement:
      kind    : 'home' | 'direct' | 'relayed' | None
      origin  : 'field' | 'infra' | None      (set by caller from src)
      role    : human-readable role string
      src     : source callsign
      path    : path string
    """
    src, dest, path = parse_header(raw)
    path_str = ",".join(path)
    if src == target:
        return {"kind": "home", "src": src, "path": path_str, "role": "own beacon"}
    digipath, igate, qcode = split_q(path)
    used = used_digis(digipath)
    first_hop = used[0] if used else igate
    is_digi = target in used
    is_igate = (target == igate)
    handled = is_digi or (is_igate and (qcode in RF_Q or include_net))
    if not handled:
        return {"kind": None, "src": src, "path": path_str}
    roles = []
    if is_digi:
        roles.append("digipeated")
    if is_igate:
        roles.append(f"igated {qcode}")
    kind = "direct" if target == first_hop else "relayed"
    return {"kind": kind, "src": src, "path": path_str, "role": "+".join(roles)}

# ------------------------------------------------------------------ icons ---

def _dot(size=64):
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    pad = size * 0.18
    d.ellipse([pad, pad, size - pad, size - pad], fill=(255, 255, 255, 255),
              outline=(0, 0, 0, 180), width=max(1, size // 24))
    buf = BytesIO(); img.save(buf, "PNG"); return buf.getvalue()

def _ring(size=64):
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    pad = size * 0.12
    d.ellipse([pad, pad, size - pad, size - pad], fill=(0, 0, 0, 0),
              outline=(255, 255, 255, 255), width=max(2, size // 8))
    buf = BytesIO(); img.save(buf, "PNG"); return buf.getvalue()

DOT_PNG = _dot()
HOME_PNG = _ring()

# ------------------------------------------------------------------ colors ---

# Visually distinct categorical palette (name, RRGGBB). One hue per station,
# used for its dots AND its home ring so color alone identifies who handled what.
PALETTE = [
    ("red",     "e6194b"),
    ("green",   "3cb44b"),
    ("blue",    "4363d8"),
    ("orange",  "f58231"),
    ("purple",  "911eb4"),
    ("cyan",    "42d4f4"),
    ("magenta", "f032e6"),
    ("teal",    "469990"),
    ("brown",   "9a6324"),
    ("navy",    "000075"),
    ("lime",    "bfef45"),
    ("pink",    "fabed4"),
]

def kml_color(hex6, alpha):
    """RRGGBB + 2-char alpha -> KML aabbggrr."""
    r, g, b = hex6[0:2], hex6[2:4], hex6[4:6]
    return f"{alpha}{b}{g}{r}".lower()

# -------------------------------------------------------------------- kml ---

def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def dot_pm(kind, src, ts, role, path_str, lon, lat):
    style = "direct" if kind == "direct" else "relayed"
    desc = (f"{esc(src)}<br/>{ts:%Y-%m-%d %H:%M:%S}z<br/>{esc(role)}<br/>"
            f"<span style='color:#888'>{esc(path_str)}</span>")
    return (f'<Placemark><styleUrl>#{style}</styleUrl>'
            f'<description><![CDATA[{desc}]]></description>'
            f'<Point><altitudeMode>clampToGround</altitudeMode>'
            f'<coordinates>{lon:.5f},{lat:.5f},0</coordinates></Point></Placemark>')

def build_kml(call, home, field, infra, dscale, rscale, hex6):
    direct_c = kml_color(hex6, "ff")   # opaque    = heard direct
    relayed_c = kml_color(hex6, "70")  # faint     = relayed via another hop
    home_c = kml_color(hex6, "ff")     # same hue as its dots
    styles = f"""<Style id="direct"><IconStyle><color>{direct_c}</color><scale>{dscale}</scale>
  <Icon><href>files/dot.png</href></Icon></IconStyle><LabelStyle><scale>0</scale></LabelStyle></Style>
<Style id="relayed"><IconStyle><color>{relayed_c}</color><scale>{rscale}</scale>
  <Icon><href>files/dot.png</href></Icon></IconStyle><LabelStyle><scale>0</scale></LabelStyle></Style>
<Style id="home"><IconStyle><color>{home_c}</color><scale>1.5</scale>
  <Icon><href>files/home.png</href></Icon></IconStyle>
  <LabelStyle><color>{home_c}</color><scale>0.9</scale></LabelStyle></Style>
"""
    home_pm = ""
    if home:
        home_pm = (f'<Placemark><name>{esc(call)}</name><styleUrl>#home</styleUrl>'
                   f'<description><![CDATA[Site position (own beacon)<br/>'
                   f'{home["ts"]:%Y-%m-%d %H:%M:%S}z]]></description>'
                   f'<Point><coordinates>{home["lon"]:.5f},{home["lat"]:.5f},0'
                   f'</coordinates></Point></Placemark>')
    field_folder = (f'<Folder><name>Field traffic handled ({len(field)})</name>\n'
                    + "\n".join(field) + "\n</Folder>")
    infra_folder = (f'<Folder><name>Infrastructure beacons handled ({len(infra)})</name>'
                    f'<visibility>0</visibility>\n' + "\n".join(infra) + "\n</Folder>")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
<Document id="aprs">
<name>{esc(call)} traffic</name>
{styles}{home_pm}
{field_folder}
{infra_folder}
</Document>
</kml>
"""

def write_kmz(path, kml):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"{path.stem}.kml", kml)
        z.writestr("files/dot.png", DOT_PNG)
        z.writestr("files/home.png", HOME_PNG)

# ------------------------------------------------------------------- main ---

def main():
    ap = argparse.ArgumentParser(description="Per-digi/igate traffic KMZ from Valkey")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--db", type=int, default=0)
    ap.add_argument("--stream", default="aprsis")
    ap.add_argument("--calls", required=True, help="comma-separated station callsign-SSIDs")
    ap.add_argument("--infra", default="",
                    help="extra fixed stations to treat as infrastructure origin "
                         "(comma-separated); the --calls stations are always infra")
    ap.add_argument("--start", type=parse_arg_time, default=None, help="ISO-8601, UTC if no offset")
    ap.add_argument("--end", type=parse_arg_time, default=None)
    ap.add_argument("--out-dir", default="digi_kmz")
    ap.add_argument("--include-net", action="store_true",
                    help="also credit igates for net-sourced packets (qAS/qAC/qAI), not just RF")
    ap.add_argument("--direct-only", action="store_true",
                    help="only map packets the station heard directly off-air (drop relayed)")
    ap.add_argument("--direct-scale", type=float, default=0.6)
    ap.add_argument("--relayed-scale", type=float, default=0.45)
    args = ap.parse_args()

    end = args.end or datetime.now(timezone.utc)
    start = args.start or (end - timedelta(days=2))
    calls = [c.strip().upper() for c in args.calls.split(",") if c.strip()]
    extra_infra = {c.strip().upper() for c in args.infra.split(",") if c.strip()}
    infra_set = set(calls) | extra_infra
    prefixes = {c.split("-")[0] for c in calls}
    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)

    home = {c: None for c in calls}
    field = {c: [] for c in calls}
    infra = {c: [] for c in calls}
    n_dir = {c: 0 for c in calls}
    n_rel = {c: 0 for c in calls}

    r = redis.Redis(host=args.host, port=args.port, db=args.db)
    scanned = 0
    for ts, raw in iter_stream(r, args.stream, start, end):
        scanned += 1
        if not any(p in raw for p in prefixes):
            continue
        pos = None      # parse position at most once per packet
        for call in calls:
            info = analyze(raw, call, args.include_net)
            if info["kind"] is None:
                continue
            if pos is None:
                try:
                    p = aprslib.parse(raw)
                    pos = (p["latitude"], p["longitude"]) \
                        if "latitude" in p and "longitude" in p else False
                except Exception:
                    pos = False
            if not pos:
                continue
            lat, lon = pos
            if info["kind"] == "home":
                if home[call] is None:
                    home[call] = {"lat": lat, "lon": lon, "ts": ts}
                continue
            if info["kind"] == "relayed" and args.direct_only:
                continue
            pm = dot_pm(info["kind"], info["src"], ts, info["role"], info["path"], lon, lat)
            if info["src"].upper() in infra_set:
                infra[call].append(pm)
            else:
                field[call].append(pm)
            if info["kind"] == "direct":
                n_dir[call] += 1
            else:
                n_rel[call] += 1

    # deterministic color per station (same each run for a given call order)
    color_of = {c: PALETTE[i % len(PALETTE)] for i, c in enumerate(calls)}

    stamp = f"{start:%Y%m%d_%H%M%S}_{end:%Y%m%d_%H%M%S}"
    print(f"Scanned {scanned} stream entries.  "
          f"igate credit: {'RF+net' if args.include_net else 'RF only (qAR/qAO)'}\n")
    print(f"{'station':10} {'color':8} {'field':>6} {'infra':>6} {'direct':>7} {'relayed':>8} {'home':>5}   file")
    made = 0
    for call in calls:
        cname, chex = color_of[call]
        if not field[call] and not infra[call] and home[call] is None:
            print(f"{call:10} {cname:8} {'-':>6} {'-':>6} {'-':>7} {'-':>8} {'no':>5}   (nothing -- skipped)")
            continue
        kml = build_kml(call, home[call], field[call], infra[call],
                        args.direct_scale, args.relayed_scale, chex)
        out = out_dir / f"digi_{call}_{stamp}.kmz"
        write_kmz(out, kml)
        made += 1
        print(f"{call:10} {cname:8} {len(field[call]):>6} {len(infra[call]):>6} "
              f"{n_dir[call]:>7} {n_rel[call]:>8} {'yes' if home[call] else 'no':>5}   {out.name}")
    print(f"\n{made} KMZ(s) written to {out_dir}")

if __name__ == "__main__":
    main()
