#!/usr/bin/env bash
# Download a month of Jersey City Citi Bike trip data, preprocess it
# (compute `trip_seconds`), and ingest it as a Tutankhamun shard.
#
# Usage:
#   scripts/seed_citibike.sh                # default: 202301
#   scripts/seed_citibike.sh 202302         # other month
#   CACHE_DIR=/tmp/cb OUT_DIR=/tmp/cb-shard scripts/seed_citibike.sh
#
# Files are cached under .local/seed-cache so re-runs skip the
# download + preprocessing steps.

set -euo pipefail

MONTH="${1:-202301}"
CACHE_DIR="${CACHE_DIR:-.local/seed-cache}"
OUT_DIR="${OUT_DIR:-.local/storage/citibike/jc-$MONTH}"

# Rustup puts cargo on PATH via shell init files; pull it in explicitly
# so the script works from non-interactive shells too.
[[ -f "$HOME/.cargo/env" ]] && source "$HOME/.cargo/env"

for tool in curl unzip python3 cargo; do
    command -v "$tool" >/dev/null || { echo "error: $tool not found in PATH"; exit 1; }
done

mkdir -p "$CACHE_DIR"

ZIP="$CACHE_DIR/JC-$MONTH-citibike-tripdata.csv.zip"
CSV="$CACHE_DIR/JC-$MONTH-citibike-tripdata.csv"
CSV_DUR="$CACHE_DIR/JC-$MONTH-citibike-tripdata-with-duration.csv"

if [[ ! -f "$ZIP" ]]; then
    URL="https://s3.amazonaws.com/tripdata/JC-$MONTH-citibike-tripdata.csv.zip"
    echo ">>> downloading $URL"
    curl -sLf -o "$ZIP" "$URL" || { echo "download failed (month $MONTH may not exist)"; exit 1; }
fi

if [[ ! -f "$CSV" ]]; then
    echo ">>> unzipping"
    unzip -o "$ZIP" "JC-$MONTH-citibike-tripdata.csv" -d "$CACHE_DIR" >/dev/null
fi

if [[ ! -f "$CSV_DUR" ]]; then
    echo ">>> adding trip_seconds column"
    python3 - "$CSV" "$CSV_DUR" <<'PY'
import csv
import sys
from datetime import datetime

fmt = "%Y-%m-%d %H:%M:%S"
with open(sys.argv[1]) as fin, open(sys.argv[2], "w") as fout:
    r = csv.reader(fin)
    w = csv.writer(fout)
    header = next(r)
    w.writerow(header + ["trip_seconds"])
    si = header.index("started_at")
    ei = header.index("ended_at")
    skipped = 0
    for row in r:
        try:
            t1 = datetime.strptime(row[si], fmt)
            t2 = datetime.strptime(row[ei], fmt)
            w.writerow(row + [max(0, int((t2 - t1).total_seconds()))])
        except (ValueError, IndexError):
            skipped += 1
    if skipped:
        print(f"skipped {skipped} rows with bad timestamps", file=sys.stderr)
PY
fi

echo ">>> ingesting into $OUT_DIR (one shard per day)"
cargo run --release --quiet --bin t9n -- ingest "$CSV_DUR" \
    --output "$OUT_DIR" \
    --time started_at \
    --metric trip_seconds \
    --string rideable_type \
    --string member_casual \
    --string start_station_id \
    --string end_station_id \
    --shard-by daily

cat <<EOF

dataset ready at: $OUT_DIR
  (one shard per day: $OUT_DIR/YYYY-MM-DD/)

try:
  cargo run --release --bin t9n -- shard inspect $OUT_DIR/2023-01-01
  cargo run --release --bin t9n -- query $OUT_DIR --metric trip_seconds
  cargo run --release --bin t9n -- query $OUT_DIR \\
      --filter member_casual=member --metric trip_seconds
  cargo run --release --bin t9n -- query $OUT_DIR \\
      --filter rideable_type=electric_bike --metric trip_seconds
EOF
