#!/usr/bin/env bash
# Upload the immutable landing zone to the Unity Catalog volume used by the Databricks job.
# Requires the Databricks CLI, authenticated by you (`databricks auth login`).
set -euo pipefail
cd "$(dirname "$0")/.."
VOLUME="${1:-/Volumes/workspace/solaratlas/data}"
databricks fs cp -r --overwrite data/landing "dbfs:${VOLUME}/landing"
echo "uploaded data/landing -> ${VOLUME}/landing"
