#!/usr/bin/env bash
# Download N months of NYC TLC yellow taxi trip records (Parquet) and stage
# them under .local/storage/nyc_taxi/. Idempotent: skips files that already
# exist.
#
# Months can be overridden via $TAXI_MONTHS, e.g.:
#   TAXI_MONTHS="2024-01 2024-02 2024-03 2024-04" bash .local/seed/nyc-taxi/fetch.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
DEST_DIR="$PROJECT_ROOT/.local/storage/nyc_taxi"

# Default to three months from early 2024 — a known-stable archived range.
MONTHS="${TAXI_MONTHS:-2024-01 2024-02 2024-03}"
BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"

mkdir -p "$DEST_DIR"

echo "Fetching NYC taxi yellow trips into $DEST_DIR"
echo "Months: $MONTHS"
echo

for month in $MONTHS; do
  filename="yellow_tripdata_${month}.parquet"
  url="${BASE_URL}/${filename}"
  out="${DEST_DIR}/${filename}"
  tmp="${out}.partial"

  # Discard any leftover sidecar from a previous interrupted run.
  rm -f "$tmp"

  if [[ -s "$out" ]]; then
    size=$(du -h "$out" | cut -f1)
    echo "  [skip] $filename already present ($size)"
    continue
  fi

  echo "  [get]  $url"
  # Download to a .partial sidecar and rename on success so an interrupted
  # curl can't leave a half-downloaded file that the next run silently skips.
  curl --fail --location --silent --show-error --output "$tmp" "$url"
  mv "$tmp" "$out"
  size=$(du -h "$out" | cut -f1)
  echo "         → $out ($size)"
done

echo
echo "Done. Storage contents:"
ls -lh "$DEST_DIR"
