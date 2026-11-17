#!/usr/bin/env python3
"""
add_packet_dots.py - Add a dot at every received packet to aprs.fi-style KMZ
tracks, so coverage gaps (long line segments with no dots) become visible in
Google Earth.

For each input KMZ it adds a "Received packets" folder containing one Point
per vertex of the track LineString, styled with a SMALL dot icon that is
EMBEDDED in the KMZ (files/dot.png) -- no external URL, so it renders reliably
in Google Earth Pro and Web. Dots are tinted to match each track's color.

Requires:  pip install pillow

Usage:
  # one file
  python3 add_packet_dots.py export_W1CMS-1_*.kmz -o out/

  # a whole directory of KMZs
  python3 add_packet_dots.py /path/to/kmz_dir -o out/

  # tune dot size and outline
  python3 add_packet_dots.py kmz_dir -o out/ --scale 0.6
"""

import argparse
import re
import sys
import zipfile
from io import BytesIO
from pathlib import Path

try:
    from PIL import Image, ImageDraw
except ImportError:
    sys.exit("Pillow is required:  pip install pillow")

LINE_RE = re.compile(r"<LineString>.*?<coordinates>\s*(.*?)\s*</coordinates>", re.S)
LINECOLOR_RE = re.compile(r"<LineStyle>.*?<color>(\w{8})</color>", re.S)

def make_dot_png(size=64):
    """White dot with a thin dark outline; tinted per-station via IconStyle color."""
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    pad = size * 0.18
    d.ellipse([pad, pad, size - pad, size - pad],
              fill=(255, 255, 255, 255),
              outline=(0, 0, 0, 180), width=max(1, size // 24))
    buf = BytesIO()
    img.save(buf, "PNG")
    return buf.getvalue()

DOT_PNG = make_dot_png()

def process(kmz_path: Path, out_dir: Path, scale: float) -> int:
    zin = zipfile.ZipFile(kmz_path)
    names = zin.namelist()
    kml_names = [n for n in names if n.lower().endswith(".kml")]
    if not kml_names:
        print(f"  {kmz_path.name}: no KML inside -- skipped")
        return 0
    kml_name = kml_names[0]
    kml = zin.read(kml_name).decode("utf-8", "replace")

    m = LINE_RE.search(kml)
    if not m:
        print(f"  {kmz_path.name}: no LineString/track -- copied unchanged")
        _copy_through(kmz_path, out_dir)
        return 0
    coords = m.group(1).split()

    cm = LINECOLOR_RE.search(kml)
    color = cm.group(1) if cm else "ff00ffff"

    style = (f'<Style id="pktdot"><IconStyle><color>{color}</color>'
             f'<scale>{scale}</scale><Icon><href>files/dot.png</href></Icon>'
             f'</IconStyle><LabelStyle><scale>0</scale></LabelStyle></Style>\n')

    n = len(coords)
    dots = []
    for i, c in enumerate(coords, 1):
        parts = c.split(",")
        lon, lat = parts[0], parts[1]
        dots.append(
            f'<Placemark><styleUrl>#pktdot</styleUrl>'
            f'<description>packet {i} of {n}</description>'
            f'<Point><altitudeMode>clampToGround</altitudeMode>'
            f'<coordinates>{lon},{lat},0</coordinates></Point></Placemark>')
    folder = f'<Folder><name>Received packets ({n})</name>\n' + "\n".join(dots) + "\n</Folder>\n"

    # inject style before the first placemark, folder before document close
    kml = kml.replace("<Placemark>", style + "<Placemark>", 1)
    kml = kml.replace("</Document>", folder + "</Document>")

    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / kmz_path.name
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as zo:
        zo.writestr(kml_name, kml)
        for other in names:
            if other != kml_name:
                zo.writestr(other, zin.read(other))
        zo.writestr("files/dot.png", DOT_PNG)
    print(f"  {kmz_path.name}: {n} packet dots added")
    return n

def _copy_through(kmz_path: Path, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / kmz_path.name).write_bytes(kmz_path.read_bytes())

def main():
    ap = argparse.ArgumentParser(description="Add per-packet dots to aprs.fi-style KMZs")
    ap.add_argument("inputs", nargs="+", help="KMZ file(s) or a directory of them")
    ap.add_argument("-o", "--out-dir", default="kmz_with_dots")
    ap.add_argument("--scale", type=float, default=0.5, help="dot icon scale (default 0.5)")
    args = ap.parse_args()

    files = []
    for item in args.inputs:
        p = Path(item)
        if p.is_dir():
            files.extend(sorted(p.glob("*.kmz")))
        elif p.is_file():
            files.append(p)
        else:
            print(f"  (skip) not found: {item}")
    # ignore macOS resource-fork junk
    files = [f for f in files if not f.name.startswith("._")]
    if not files:
        sys.exit("No KMZ files found.")

    out_dir = Path(args.out_dir)
    total = sum(process(f, out_dir, args.scale) for f in files)
    print(f"\n{len(files)} KMZ(s) processed, {total} dots total -> {out_dir}")

if __name__ == "__main__":
    main()
