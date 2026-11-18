#!/usr/bin/env bash
# run_day.sh <friday|saturday> — run the APRS analysis for one event day,
# limited to the ACTUAL event window (local Eastern time, EDT = UTC-4), so
# driving in from home and back is excluded.
#
# Event windows (from the net logs):
#   Friday    05:30 – 15:19 EDT   (The Prouty Ultimate)
#   Saturday  05:30 – 16:10 EDT   (The Prouty)
#
# Produces, under output/2026/<day>/:
#   per-tracker track KMZ + combined overlay, anomaly KMZ,
#   health.txt + tracker_health/events CSVs, telemetry.txt + telemetry CSV,
#   <short>-digi.kmz (per-day digipeater/iGate coverage, toggleable by station).
#
# Usage:
#   bash tools/run_day.sh friday
#   bash tools/run_day.sh saturday
set -euo pipefail
cd "$(dirname "$0")/.."                 # repo root

# Our fixed digipeaters / iGates on the air that weekend (same both days).
# Edit here if the infrastructure list changes.
STATIONS="N1GMC-1,N1GMC-2,W1FN-1,W1FN-3,W1FN-5,W1FN-9,WX1NH-10"

day="${1:-}"
case "$day" in
  friday)   start="2026-07-10T05:30:00-04:00"; end="2026-07-10T15:19:00-04:00"; short=fri ;;
  saturday) start="2026-07-11T05:30:00-04:00"; end="2026-07-11T16:10:00-04:00"; short=sat ;;
  *) echo "usage: bash tools/run_day.sh friday|saturday"; exit 2 ;;
esac

roster="config/$day.csv"
out="output/2026/$day"
mkdir -p "$out"
echo "== $day : $start .. $end (event window) =="

python3 tools/prouty_kmz.py --roster "$roster" --combined --combined-name "$short-overlay" \
  --start "$start" --end "$end" --out-dir "$out"

python3 tools/tracker_health.py --roster "$roster" \
  --start "$start" --end "$end" --min-voltage 11.5 --kmz --out-dir "$out" \
  | tee "$out/health.txt"

python3 tools/telemetry_decode.py --roster "$roster" \
  --start "$start" --end "$end" --min-voltage 11.5 --csv --out-dir "$out" \
  | tee "$out/telemetry.txt"

# per-day digipeater/iGate coverage: one combined KMZ, one toggleable layer per
# station, for the digi web map (fri-digi.kmz / sat-digi.kmz).
python3 tools/digi_kmz.py --calls "$STATIONS" --combined --combined-name "$short-digi" \
  --title "Prouty $day — digipeater / iGate coverage" \
  --start "$start" --end "$end" --out-dir "$out" \
  | tee "$out/digi.txt"

echo "== done: $out =="
