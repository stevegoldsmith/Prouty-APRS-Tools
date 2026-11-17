#!/usr/bin/env python3
"""
prouty_kmz.py - Build aprs.fi-style KMZ track files from a Valkey APRS stream.

Reads raw packets from the `aprsis` stream (fields: timestamp, packet),
filters to the callsigns you care about, and writes ONE KMZ PER CALLSIGN,
structurally matching an aprs.fi export:

  - KML 2.1, one Style + one Placemark per station
  - MultiGeometry: Point at last-heard position + LineString of the track
  - Absolute altitudes, description with time / MPH / course / feet / comment
  - APRS symbol icon embedded at files/sym/f24/<name>.png

Symbol icons: if you have the aprs-symbols sprite sheets from
https://github.com/hessu/aprs-symbols (aprs-symbols-24-0.png and
aprs-symbols-24-1.png) in --symbols DIR and Pillow installed, the correct
per-station symbol is cropped and embedded, just like aprs.fi. Otherwise a
generic dot icon URL is referenced instead.

Requires:  pip install redis aprslib      (Pillow optional, for symbols)

Usage:
  python3 prouty_kmz.py --calls W1CMS-1,W1CMS-2,KC1XYZ-9 \
      --start 2026-07-11T08:00:00 --end 2026-07-11T20:00:00 \
      --out-dir ./prouty_kmz --symbols ./aprs-symbols
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

FRAC = re.compile(r"\.(\d+)")
KMH_TO_MPH = 0.621371
M_TO_FT = 3.28084

# Track line colors (KML aabbggrr), cycled per callsign like aprs.fi's palette
LINE_COLORS = ["ff00ff00", "ff0000ff", "ffff0000", "ff00ffff",
               "ffff00ff", "ffffff00", "ff0080ff", "ff8000ff"]

def parse_iso(ts: str) -> datetime:
    ts = ts.strip().replace("Z", "+00:00")
    ts = FRAC.sub(lambda m: "." + m.group(1)[:6], ts, count=1)
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def parse_arg_time(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def iter_stream(r, stream, start, end, batch=5000):
    cursor, hi = f"{ms(start)}-0", f"{ms(end)}-{2**64-1}"
    while True:
        entries = r.xrange(stream, min=cursor, max=hi, count=batch)
        if not entries:
            break
        for entry_id, fields in entries:
            def get(k):
                v = fields.get(k.encode()) if isinstance(next(iter(fields)), bytes) else fields.get(k)
                return v.decode() if isinstance(v, bytes) else v
            ts_field = get("timestamp")
            pkt = get("packet")
            if pkt is None:
                continue
            ts = parse_iso(ts_field) if ts_field else \
                datetime.fromtimestamp(int(str(entry_id).split(b"-" if isinstance(entry_id, bytes) else "-")[0]) / 1000,
                                       tz=timezone.utc)
            yield ts, pkt
        last_id = entries[-1][0]
        if isinstance(last_id, bytes):
            last_id = last_id.decode()
        cursor = "(" + last_id
        if len(entries) < batch:
            break

def collect_tracks(r, stream, start, end, calls):
    """Return {callsign: [point dicts]} for the requested callsigns."""
    wanted = {c.upper() for c in calls}
    tracks = {c: [] for c in wanted}
    parsed = unparsed = 0
    for ts, pkt in iter_stream(r, stream, start, end):
        # cheap pre-filter before full parse: source call is before '>'
        src = pkt.split(">", 1)[0].upper()
        if src not in wanted:
            continue
        try:
            p = aprslib.parse(pkt)
            parsed += 1
        except Exception:
            unparsed += 1
            continue
        if "latitude" not in p or "longitude" not in p:
            continue
        pt = {
            "time": ts,
            "lat": p["latitude"],
            "lon": p["longitude"],
            "alt": p.get("altitude"),                 # meters or None
            "speed": p.get("speed"),                  # km/h or None
            "course": p.get("course"),
            "comment": (p.get("comment") or "").strip(),
            "symbol": p.get("symbol", ">"),
            "table": p.get("symbol_table", "/"),
        }
        pts = tracks[src]
        # drop consecutive exact-duplicate coordinates (digipeated copies)
        if pts and pts[-1]["lat"] == pt["lat"] and pts[-1]["lon"] == pt["lon"] \
                and pts[-1]["alt"] == pt["alt"]:
            pts[-1]["time"] = pt["time"]  # keep freshest heard time
            continue
        pts.append(pt)
    return tracks, unparsed

# ---------------------------------------------------------------- symbols ---

def symbol_icon_name(table: str, symbol: str) -> str:
    """Mimic aprs.fi's naming, e.g. primary '>' -> P3eP3e.png"""
    t = "P" if table == "/" else "A"
    h = f"{ord(symbol):02x}"
    return f"{t}{h}{t}{h}.png"

def crop_symbol(symbols_dir: Path, table: str, symbol: str):
    """Crop a 24x24 icon from the hessu/aprs-symbols sheets. None on failure."""
    if not symbols_dir:
        return None
    try:
        from PIL import Image
    except ImportError:
        return None
    sheet_idx = 0 if table == "/" else 1
    sheet = symbols_dir / f"aprs-symbols-24-{sheet_idx}.png"
    if not sheet.exists():
        # try @2x sheet and downscale
        sheet = symbols_dir / f"aprs-symbols-24-{sheet_idx}@2x.png"
        scale = 2
        if not sheet.exists():
            return None
    else:
        scale = 1
    idx = ord(symbol) - 33
    if not (0 <= idx < 96):
        return None
    col, row = idx % 16, idx // 16
    cell = 24 * scale
    img = Image.open(sheet).crop((col * cell, row * cell,
                                  (col + 1) * cell, (row + 1) * cell))
    if scale != 1:
        img = img.resize((24, 24), Image.LANCZOS)
    buf = BytesIO()
    img.save(buf, "PNG")
    return buf.getvalue()

# -------------------------------------------------------------------- kml ---

def esc(s: str) -> str:
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))

def build_kml(call, pts, line_color, icon_href):
    last = pts[-1]
    alt_carry = 0.0
    coords = []
    for p in pts:
        if p["alt"] is not None:
            alt_carry = p["alt"]
        coords.append(f"{p['lon']:.5f},{p['lat']:.5f},{alt_carry:.2f}")
    last_alt = last["alt"] if last["alt"] is not None else alt_carry

    desc_bits = [f"{last['time']:%Y-%m-%d %H:%M:%S}z<br />"]
    line2 = []
    if last["speed"] is not None:
        line2.append(f"{last['speed'] * KMH_TO_MPH:.0f} MPH")
    if last["course"] is not None:
        line2.append(f"{last['course']:.0f}&#176;")
    if last["alt"] is not None:
        line2.append(f"{last['alt'] * M_TO_FT:.0f} ft")
    if line2:
        desc_bits.append("\n ".join(line2) + "\n<br />")
    if last["comment"]:
        desc_bits.append(
            f"<span style='color:#0a7100; font-style:italic;'>{esc(last['comment'])}</span><br />")
    description = "\n".join(desc_bits)
    snippet = esc(last["comment"])[:80] if last["comment"] else ""

    return f"""<?xml version="1.0" encoding="UTF-8"?>

<kml xmlns="http://earth.google.com/kml/2.1">
<Document id="aprs">
<Style id="t{call.replace('-', '')}">
  <LabelStyle><color>ffa8ff00</color><scale>0.75</scale></LabelStyle>
  <IconStyle>
    <Icon>
      <href>{icon_href}</href>
    </Icon>
  <scale>0.75</scale>
  </IconStyle>
  <LineStyle>
    <color>{line_color}</color>
    <colorMode>normal</colorMode>
    <width>2</width>
  </LineStyle>
  <PolyStyle><color>7f{line_color[2:]}</color></PolyStyle>
</Style>
<Placemark>
  <name>{esc(call)}</name>
  <description><![CDATA[
{description}
]]></description>
  <Snippet maxLines='2'>{snippet}</Snippet>
  <styleUrl>#t{call.replace('-', '')}</styleUrl>
  <MultiGeometry>
  <Point>
  <altitudeMode>absolute</altitudeMode>
    <coordinates>{last['lon']:.5f},{last['lat']:.5f},{last_alt:.2f}</coordinates>
  </Point>
  <LineString>
    <tessellate>1</tessellate>
    <extrude>1</extrude>
    <altitudeMode>absolute</altitudeMode>
    <coordinates>
 {' '.join(coords)}    </coordinates>
  </LineString>
  </MultiGeometry>
</Placemark>
</Document>
</kml>
"""

def write_kmz(path: Path, kml: str, icon_name, icon_bytes):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(path.stem + ".kml", kml)
        if icon_bytes:
            z.writestr(f"files/sym/f24/{icon_name}", icon_bytes)

# --------------------------------------------------- tactical roster / labels ---

def load_roster(path):
    """CSV with columns callsign,tactical for ONE day's assignments.
    Returns {CALLSIGN: tactical}. Run Friday with the Friday roster,
    Saturday with the Saturday roster (one file per day matches the
    per-day run model)."""
    import csv
    roster = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            cs = (row.get("callsign") or row.get("Callsign") or "").strip().upper()
            tac = (row.get("tactical") or row.get("Tactical") or "").strip()
            if cs and tac:
                roster[cs] = tac
    return roster

def label_for(call, roster):
    tac = roster.get(call.upper())
    return f"{tac} ({call})" if tac else call

# ------------------------------------------------------- combined overlay KML ---

def track_folder(call, label, pts, line_color, icon_href):
    """One tracker as a <Folder> (track + last-point) for the combined overlay."""
    alt_carry = 0.0
    coords = []
    for p in pts:
        if p["alt"] is not None:
            alt_carry = p["alt"]
        coords.append(f"{p['lon']:.5f},{p['lat']:.5f},{alt_carry:.2f}")
    last = pts[-1]
    last_alt = last["alt"] if last["alt"] is not None else alt_carry
    sid = "t" + call.replace("-", "")
    return f"""<Folder><name>{esc(label)}</name>
<Style id="{sid}">
  <LabelStyle><color>ffa8ff00</color><scale>0.75</scale></LabelStyle>
  <IconStyle><Icon><href>{icon_href}</href></Icon><scale>0.75</scale></IconStyle>
  <LineStyle><color>{line_color}</color><width>2</width></LineStyle>
  <PolyStyle><color>7f{line_color[2:]}</color></PolyStyle>
</Style>
<Placemark>
  <name>{esc(label)}</name>
  <styleUrl>#{sid}</styleUrl>
  <MultiGeometry>
  <Point><altitudeMode>absolute</altitudeMode>
    <coordinates>{last['lon']:.5f},{last['lat']:.5f},{last_alt:.2f}</coordinates></Point>
  <LineString><tessellate>1</tessellate><extrude>1</extrude>
    <altitudeMode>absolute</altitudeMode>
    <coordinates>
 {' '.join(coords)}    </coordinates>
  </LineString>
  </MultiGeometry>
</Placemark>
</Folder>"""

def build_combined_kml(title, folders):
    return (f'<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<kml xmlns="http://earth.google.com/kml/2.1">\n<Document id="aprs">\n'
            f'<name>{esc(title)}</name>\n' + "\n".join(folders) + "\n</Document>\n</kml>\n")

# ------------------------------------------------------------------- main ---

def main():
    ap = argparse.ArgumentParser(description="Per-callsign aprs.fi-style KMZ from Valkey")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--db", type=int, default=0)
    ap.add_argument("--stream", default="aprsis")
    ap.add_argument("--calls", default=None,
                    help="comma-separated callsign-SSIDs. Optional when --roster is given "
                         "(the roster's callsigns are used).")
    ap.add_argument("--start", type=parse_arg_time, default=None,
                    help="window start ISO-8601 (UTC if no offset); default end-2d")
    ap.add_argument("--end", type=parse_arg_time, default=None,
                    help="window end; default now")
    ap.add_argument("--out-dir", default=".", help="directory for output KMZ files")
    ap.add_argument("--combined", action="store_true",
                    help="also write ONE overlay KMZ with every tracker (per-tracker folders, "
                         "colour-coded) so all tracks show together for the day")
    ap.add_argument("--combined-name", default="overlay",
                    help="base name for the combined overlay file (default 'overlay')")
    ap.add_argument("--roster", default=None,
                    help="CSV (callsign,tactical) of this day's tactical assignments; "
                         "labels become 'Tactical (CALLSIGN)'. Use one roster file per day.")
    ap.add_argument("--symbols", default=None,
                    help="dir containing hessu/aprs-symbols aprs-symbols-24-*.png sheets")
    args = ap.parse_args()

    end = args.end or datetime.now(timezone.utc)
    start = args.start or (end - timedelta(days=2))
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    symbols_dir = Path(args.symbols) if args.symbols else None

    roster = load_roster(args.roster) if args.roster else {}
    if args.calls:
        calls = [c.strip().upper() for c in args.calls.split(",") if c.strip()]
    elif roster:
        calls = list(roster.keys())            # roster supplies the callsign list
    else:
        sys.exit("give --calls or --roster")

    r = redis.Redis(host=args.host, port=args.port, db=args.db)
    tracks, unparsed = collect_tracks(r, args.stream, start, end, calls)

    stamp = f"{start:%Y%m%d_%H%M%S}_{end:%Y%m%d_%H%M%S}"
    made = 0
    overlay_folders = []     # for the --combined overlay
    for i, call in enumerate(calls):
        pts = tracks.get(call, [])
        if not pts:
            print(f"  {call}: no position packets in window -- skipped")
            continue
        label = label_for(call, roster)
        color = LINE_COLORS[i % len(LINE_COLORS)]
        # overlay uses a generic dot so the single file stays small and dependency-free
        overlay_href = "http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png"
        icon_bytes = crop_symbol(symbols_dir, pts[-1]["table"], pts[-1]["symbol"])
        icon_name = symbol_icon_name(pts[-1]["table"], pts[-1]["symbol"])
        icon_href = (f"files/sym/f24/{icon_name}" if icon_bytes else overlay_href)
        kml = build_kml(label, pts, color, icon_href)
        out = out_dir / f"export_{call}_{stamp}.kmz"
        write_kmz(out, kml, icon_name, icon_bytes)
        print(f"  {call}: {len(pts)} points -> {out}")
        made += 1
        if args.combined:
            overlay_folders.append(track_folder(call, label, pts, color, overlay_href))

    if args.combined and overlay_folders:
        title = f"Prouty tracks {start:%Y-%m-%d %H:%M}-{end:%Y-%m-%d %H:%M}z"
        overlay = build_combined_kml(title, overlay_folders)
        opath = out_dir / f"{args.combined_name}_{stamp}.kmz"
        with zipfile.ZipFile(opath, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr(opath.stem + ".kml", overlay)
        print(f"  overlay: {len(overlay_folders)} trackers -> {opath}")

    note = f" ({unparsed} unparseable packets skipped)" if unparsed else ""
    print(f"\n{made}/{len(calls)} KMZ files written to {out_dir}{note}")

if __name__ == "__main__":
    main()
