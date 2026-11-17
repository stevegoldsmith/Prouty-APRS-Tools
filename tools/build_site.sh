#!/usr/bin/env bash
# build_site.sh - Assemble the gate files and upload them to the production site.
#
# Builds  site/  laid out exactly as it sits behind gate.php, then scp's it to
# the server:
#
#   site/html/           -> <base>/html/            report pages + aprs-map.html
#   site/aprs/friday/    -> <base>/aprs/friday/     fri-overlay.kmz + per-tracker + anomaly
#   site/aprs/saturday/  -> <base>/aprs/saturday/   sat-overlay.kmz + per-tracker + anomaly
#
# The timestamped overlay is copied to the stable name the embedded map fetches
# (fri-overlay.kmz / sat-overlay.kmz); nothing to rename by hand.
#
# Usage:
#   bash tools/build_site.sh                 # build AND upload (default)
#   bash tools/build_site.sh --build-only    # build site/ only, no upload
#   bash tools/build_site.sh --host=user@host --base=/path   # override target
#
# Target defaults (override with the flags above or the env vars):
#   PROUTY_REMOTE=w1hs@hosting.tardisventures.net
#   PROUTY_BASE=/home/w1hs.com/private/prouty-2026
set -euo pipefail
cd "$(dirname "$0")/.."                 # repo root

REMOTE="${PROUTY_REMOTE:-w1hs@hosting.tardisventures.net}"
BASE="${PROUTY_BASE:-/home/w1hs.com/private/prouty-2026}"
UPLOAD=1
for a in "$@"; do
  case "$a" in
    --build-only|-n) UPLOAD=0 ;;
    --host=*) REMOTE="${a#*=}" ;;
    --base=*) BASE="${a#*=}" ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown arg: $a (try --help)"; exit 2 ;;
  esac
done

# ---------------------------------------------------------- build site/ ------
SITE=site
rm -rf "$SITE"
mkdir -p "$SITE/html" "$SITE/aprs/friday" "$SITE/aprs/saturday"

cp docs/2026-07-10-aprs-friday.html \
   docs/2026-07-11-aprs-saturday.html \
   docs/aprs-map.html \
   "$SITE/html/"

for day in friday saturday; do
  short=${day:0:3}                      # fri / sat
  src="output/2026/$day"
  dst="$SITE/aprs/$day"
  [ -d "$src" ] || { echo "!! missing $src -- run the $day tools first"; continue; }
  # The overlay the embedded map fetches goes in html/ beside the map (a
  # proven-working path depth); per-tracker + anomaly KMZ go in aprs/<day>/.
  ov=$(ls "$src/${short}-overlay_"*.kmz 2>/dev/null | head -1 || true)
  if [ -n "$ov" ]; then cp "$ov" "$SITE/html/${short}-overlay.kmz"
  else echo "!! no ${short}-overlay in $src -- run prouty_kmz.py --combined for $day"; fi
  cp "$src"/export_*.kmz "$dst/" 2>/dev/null || true
  cp "$src"/tracker_anomalies_*.kmz "$dst/" 2>/dev/null || true
done

echo "Built ./$SITE :"
find "$SITE" -type f | sort | sed "s#^$SITE/#  #"

# ---------------------------------------------------------- upload -----------
if [ "$UPLOAD" -eq 0 ]; then
  echo
  echo "(--build-only: not uploaded. Run without it to scp to $REMOTE:$BASE)"
  exit 0
fi

# only scp a folder that has files, so an un-run day doesn't abort the upload
upload_dir() {   # <local dir> <remote dir>
  if [ -n "$(ls -A "$1" 2>/dev/null)" ]; then
    scp "$1"/* "$REMOTE:$2/"
  fi
}

echo
echo "Uploading to $REMOTE:$BASE ..."
ssh "$REMOTE" "mkdir -p '$BASE/html' '$BASE/aprs/friday' '$BASE/aprs/saturday'"
upload_dir "$SITE/html"          "$BASE/html"
upload_dir "$SITE/aprs/friday"   "$BASE/aprs/friday"
upload_dir "$SITE/aprs/saturday" "$BASE/aprs/saturday"
echo "Done."
echo "  Reports: $BASE/html/2026-07-1{0,1}-aprs-*.html"
echo "  Map:     $BASE/html/aprs-map.html   Maps: $BASE/aprs/{friday,saturday}/"
