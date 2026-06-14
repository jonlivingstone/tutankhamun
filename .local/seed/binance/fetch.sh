#!/usr/bin/env bash
# Download Binance 1-minute klines for one or more symbols into the raw
# download cache .cache/downloads/binance/. Each Binance archive is a zip
# containing a single CSV; we unzip into the destination so consumers see
# plain CSVs.
# Idempotent: skips files that already exist.
#
# Overridable via env:
#   BINANCE_SYMBOLS  default: "BTCUSDT ETHUSDT"
#   BINANCE_MONTHS   default: "2024-01 2024-02 2024-03 2024-04 2024-05 2024-06"
#   BINANCE_INTERVAL default: "1m"

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
DEST_DIR="$PROJECT_ROOT/.cache/downloads/binance"
TMP_DIR="$DEST_DIR/.tmp"

SYMBOLS="${BINANCE_SYMBOLS:-BTCUSDT ETHUSDT}"
MONTHS="${BINANCE_MONTHS:-2024-01 2024-02 2024-03 2024-04 2024-05 2024-06}"
INTERVAL="${BINANCE_INTERVAL:-1m}"
BASE_URL="https://data.binance.vision/data/spot/monthly/klines"

mkdir -p "$DEST_DIR" "$TMP_DIR"

echo "Fetching Binance klines into $DEST_DIR"
echo "Symbols : $SYMBOLS"
echo "Months  : $MONTHS"
echo "Interval: $INTERVAL"
echo

for sym in $SYMBOLS; do
  for month in $MONTHS; do
    base="${sym}-${INTERVAL}-${month}"
    csv_out="${DEST_DIR}/${base}.csv"
    zip_tmp="${TMP_DIR}/${base}.zip"

    # Discard any leftover zip from a previous interrupted run.
    rm -f "$zip_tmp"

    if [[ -s "$csv_out" ]]; then
      size=$(du -h "$csv_out" | cut -f1)
      echo "  [skip] ${base}.csv already present ($size)"
      continue
    fi

    zip_url="${BASE_URL}/${sym}/${INTERVAL}/${base}.zip"
    echo "  [get]  $zip_url"
    curl --fail --location --silent --show-error --output "$zip_tmp" "$zip_url"
    unzip -q -o "$zip_tmp" -d "$DEST_DIR"
    rm -f "$zip_tmp"
    size=$(du -h "$csv_out" | cut -f1)
    echo "         → $csv_out ($size)"
  done
done

# Clean up the temp dir if it's empty.
rmdir "$TMP_DIR" 2>/dev/null || true

echo
echo "Done. Downloaded files:"
ls -lh "$DEST_DIR"
