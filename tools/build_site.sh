#!/usr/bin/env bash
# build_site.sh - Assemble the gate deploy folder from committed outputs.
#
# Produces  site/  laid out exactly as it must sit behind gate.php:
#
#   site/html/   the report pages + the embedded map
#   site/aprs/friday/    fri-overlay.kmz (stable name) + per-tracker + anomaly KMZ
#   site/aprs/saturday/  sat-overlay.kmz (stable name) + per-tracker + anomaly KMZ
#
# You then upload the CONTENTS of site/ into the gate's base folder (the same
# place the other html/ pages already live, e.g. .../private/prouty-2026/).
# Nothing to rename by hand: the timestamped overlay is copied to the stable
# name the embedded map looks for.
#
# Usage:  bash tools/build_site.sh
set -euo pipefail
cd "$(dirname "$0")/.."                 # repo root

SITE=site
rm -rf "$SITE"
mkdir -p "$SITE/html" "$SITE/aprs/friday" "$SITE/aprs/saturday"

# --- report pages + the map (go in html/, next to the other gate pages) ---
cp docs/2026-07-10-aprs-friday.html \
   docs/2026-07-11-aprs-saturday.html \
   docs/aprs-map.html \
   "$SITE/html/"

# --- per-day KMZ ---
for day in friday saturday; do
  short=${day:0:3}                      # fri / sat
  src="output/2026/$day"
  dst="$SITE/aprs/$day"
  [ -d "$src" ] || { echo "!! missing $src -- run the $day tools first"; continue; }

  # overlay: timestamped source -> stable name the map fetches
  ov=$(ls "$src/${short}-overlay_"*.kmz 2>/dev/null | head -1 || true)
  if [ -n "$ov" ]; then cp "$ov" "$dst/${short}-overlay.kmz"; else
    echo "!! no ${short}-overlay in $src -- run prouty_kmz.py --combined for $day"; fi

  # per-tracker tracks and the anomaly map keep their own names
  cp "$src"/export_*.kmz "$dst/" 2>/dev/null || true
  cp "$src"/tracker_anomalies_*.kmz "$dst/" 2>/dev/null || true
done

echo
echo "Built ./$SITE  -- upload its contents into the gate base folder so you get:"
echo
find "$SITE" -type f | sort | sed "s#^$SITE/#  <gate-base>/#"
echo
echo "(<gate-base> is where the existing html/ pages live, e.g. private/prouty-2026/)"
