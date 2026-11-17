#!/usr/bin/env python3
"""
aprs2kmz.py - Convert raw APRS packets to a KMZ track file (Google Earth).

Reproduces the useful content of an aprs.fi KMZ export: a time-stamped
track (gx:Track) plus per-point placemarks carrying speed, course,
altitude, and comment data parsed from the packets.

Input formats accepted (auto-detected per line):
  1. aprs.fi "raw packets" page copy/paste:
       2026-07-15 08:12:33 EDT: W1CMS-1>APDR16,WIDE1-1,qAR,K1ABC-10:!4428.12N/07312.55W>Comment
  2. APRS-IS log lines with a leading unix timestamp:
       1752570753 W1CMS-1>APDR16,WIDE1-1,qAR,K1ABC-10:!4428.12N/07312.55W>Comment
  3. Bare TNC2 lines (no timestamp -- points are kept but untimed):
       W1CMS-1>APDR16,WIDE1-1,qAR,K1ABC-10:!4428.12N/07312.55W>Comment

Requires:  pip install aprslib
Usage:     python3 aprs2kmz.py packets.txt W1CMS-1 out.kmz [--tz-offset -4]
"""

import argparse
import re
import sys
import zipfile
from datetime import datetime, timedelta, timezone
from xml.sax.saxutils import escape

try:
    import aprslib
except ImportError:
    sys.exit("aprslib is required:  pip install aprslib")

# aprs.fi raw-packet line: "2026-07-15 08:12:33 EDT: <packet>"
APRSFI_LINE = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+([A-Z]{2,5}|[+-]\d{4}):?\s+(.*)$"
)
UNIX_LINE = re.compile(r"^(\d{9,10})\s+(.*)$")

def parse_line(line, tz_offset_hours):
    """Return (datetime-or-None, packet-string) for one log line."""
    line = line.strip()
    if not line or line.startswith("#"):
        return None, None
    m = APRSFI_LINE.match(line)
    if m:
        ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
        ts = ts.replace(tzinfo=timezone(timedelta(hours=tz_offset_hours)))
        return ts.astimezone(timezone.utc), m.group(3)
    m = UNIX_LINE.match(line)
    if m:
        return datetime.fromtimestamp(int(m.group(1)), tz=timezone.utc), m.group(2)
    return None, line  # bare TNC2 packet, no timestamp

def extract_points(path, callsign, tz_offset_hours):
    points, skipped = [], 0
    with open(path, "r", errors="replace") as f:
        for line in f:
            ts, pkt = parse_line(line, tz_offset_hours)
            if not pkt:
                continue
            try:
                p = aprslib.parse(pkt)
            except Exception:
                skipped += 1
                continue
            if callsign and p.get("from", "").upper() != callsign.upper():
                continue
            if "latitude" not in p or "longitude" not in p:
                continue  # status/telemetry/message packets
            points.append({
                "time": ts,
                "lat": p["latitude"],
                "lon": p["longitude"],
                "alt": p.get("altitude"),          # meters
                "speed": p.get("speed"),           # km/h
                "course": p.get("course"),
                "comment": p.get("comment", ""),
            })
    # sort by time; untimed points keep file order at the end
    timed = sorted([p for p in points if p["time"]], key=lambda p: p["time"])
    untimed = [p for p in points if not p["time"]]
    return timed + untimed, skipped

def kml_for(points, callsign):
    name = escape(callsign or "APRS track")
    whens, coords, placemarks = [], [], []
    for p in points:
        alt = p["alt"] if p["alt"] is not None else 0
        coords.append(f"<gx:coord>{p['lon']:.6f} {p['lat']:.6f} {alt:.1f}</gx:coord>")
        t = p["time"].strftime("%Y-%m-%dT%H:%M:%SZ") if p["time"] else ""
        whens.append(f"<when>{t}</when>")
        desc = []
        if p["time"]:   desc.append(f"Time: {p['time']:%Y-%m-%d %H:%M:%S} UTC")
        if p["speed"] is not None:  desc.append(f"Speed: {p['speed']:.1f} km/h")
        if p["course"] is not None: desc.append(f"Course: {p['course']}&#176;")
        if p["alt"] is not None:    desc.append(f"Altitude: {p['alt']:.0f} m")
        if p["comment"]:            desc.append(f"Comment: {escape(p['comment'])}")
        placemarks.append(
            "<Placemark><styleUrl>#pt</styleUrl>"
            f"<description><![CDATA[{'<br/>'.join(desc)}]]></description>"
            + (f"<TimeStamp><when>{t}</when></TimeStamp>" if t else "")
            + f"<Point><coordinates>{p['lon']:.6f},{p['lat']:.6f},{alt:.1f}"
              "</coordinates></Point></Placemark>"
        )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2">
<Document>
  <name>{name}</name>
  <Style id="track">
    <LineStyle><color>ff0000ff</color><width>3</width></LineStyle>
    <IconStyle><scale>0.8</scale></IconStyle>
  </Style>
  <Style id="pt">
    <IconStyle><scale>0.5</scale>
      <Icon><href>http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png</href></Icon>
    </IconStyle>
    <LabelStyle><scale>0</scale></LabelStyle>
  </Style>
  <Folder><name>{name} track</name>
    <Placemark>
      <name>{name}</name>
      <styleUrl>#track</styleUrl>
      <gx:Track>
        <altitudeMode>clampToGround</altitudeMode>
        {'\n        '.join(whens)}
        {'\n        '.join(coords)}
      </gx:Track>
    </Placemark>
  </Folder>
  <Folder><name>Points</name>
    {'\n    '.join(placemarks)}
  </Folder>
</Document>
</kml>
"""

def main():
    ap = argparse.ArgumentParser(description="Raw APRS packets -> KMZ track")
    ap.add_argument("input", help="raw packet log file")
    ap.add_argument("callsign", nargs="?", default=None,
                    help="filter to this callsign-SSID (e.g. W1CMS-1)")
    ap.add_argument("output", nargs="?", default="track.kmz")
    ap.add_argument("--tz-offset", type=float, default=0.0,
                    help="hours offset of the log's timestamps from UTC "
                         "(aprs.fi shows YOUR local time; EDT = -4)")
    args = ap.parse_args()

    points, skipped = extract_points(args.input, args.callsign, args.tz_offset)
    if not points:
        sys.exit("No position packets found (check callsign filter / input format).")

    kml = kml_for(points, args.callsign)
    with zipfile.ZipFile(args.output, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("doc.kml", kml)

    print(f"{len(points)} positions written to {args.output}"
          + (f" ({skipped} unparseable packets skipped)" if skipped else ""))

if __name__ == "__main__":
    main()
