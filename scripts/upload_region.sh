#!/usr/bin/env bash
# Upload only what one non-tiled region needs (its buildings + shared context), not the whole state.
#   scripts/upload_region.sh coimbatore [/Volumes/...]
set -euo pipefail
cd "$(dirname "$0")/.."
REGION="${1:?usage: upload_region.sh <region> [volume]}"
VOLUME="${2:-/Volumes/workspace/solaratlas/data}"
REL="release=$(awk '/release:/{gsub(/"/,"",$2); print $2; exit}' config/sources.yaml)"
put() { [ -e "data/landing/$1" ] || { echo "missing data/landing/$1" >&2; exit 1; }
        databricks fs mkdirs "dbfs:${VOLUME}/landing/$1"
        databricks fs cp -r --overwrite "data/landing/$1" "dbfs:${VOLUME}/landing/$1"; }
put "buildings/${REL}/region=${REGION}"
for d in divisions places land_use; do put "${d}/${REL}/state=IN-TN"; done
put irradiance/nasa_power_climatology
put irradiance/nasa_power_hourly_2025
echo "uploaded ${REGION} -> ${VOLUME}/landing"
