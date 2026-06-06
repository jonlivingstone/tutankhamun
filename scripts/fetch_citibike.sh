#!/usr/bin/env bash
# Fetch a month of Jersey City Citi Bike trip data and preprocess it
# (compute a `trip_seconds` metric column). Leaves a ready-to-ingest CSV
# and prints the `t9n ingest` command — it does not ingest itself, so the
# tutorial's ingest step stays explicit.
#
# Usage:
#   scripts/fetch_citibike.sh               # default: 202301
#   scripts/fetch_citibike.sh 202302        # other month
#   CACHE_DIR=/tmp/cb scripts/fetch_citibike.sh
#
# Files are cached under .local/seed-cache so re-runs skip the
# download + preprocessing steps.

set -euo pipefail

MONTH="${1:-202301}"
CACHE_DIR="${CACHE_DIR:-.local/seed-cache}"

for tool in curl unzip python3; do
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

cat <<EOF

data ready: $CSV_DUR

ingest it (one immutable shard per day) with:

  cargo run --release --bin t9n -- ingest $CSV_DUR \\
      --output .local/storage/citibike/jc-$MONTH \\
      --time started_at \\
      --metric trip_seconds \\
      --string rideable_type --string member_casual \\
      --string start_station_id --string end_station_id \\
      --shard-by daily
EOF
