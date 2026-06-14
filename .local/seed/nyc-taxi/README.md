# NYC TLC yellow taxi trip records

## Source

NYC Taxi & Limousine Commission, published as part of NYC Open Data:

- Landing page: <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>
- Direct file URL (one per service-month):
  `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_<YYYY-MM>.parquet`

## Usage

```sh
# Default: 2024-01, 2024-02, 2024-03 (~150–200 MB total)
bash .local/seed/nyc-taxi/fetch.sh

# Override the months
TAXI_MONTHS="2023-10 2023-11 2023-12 2024-01" \
  bash .local/seed/nyc-taxi/fetch.sh
```

Downloaded to `.cache/downloads/nyc_taxi/yellow_tripdata_<YYYY-MM>.parquet`
(raw source; ingestion turns these into shards under `.cache/storage/`).

## Ingest

`t9n ingest` reads Parquet directly — no conversion step. Integer columns load
as-is; the `double` dollar/distance columns are stored as **scaled integers**
(`--scale fare_amount=2` → cents; floats default to scale 3). The scale is
recorded in the shard metadata. `--shard-by daily` writes one shard per day:

```sh
cargo run --release --bin t9n -- ingest \
    .cache/downloads/nyc_taxi/yellow_tripdata_2024-01.parquet \
    --output .cache/storage/nyc_taxi \
    --time tpep_pickup_datetime \
    --int VendorID --int PULocationID --int DOLocationID --int payment_type \
    --metric fare_amount   --scale fare_amount=2 \
    --metric tip_amount    --scale tip_amount=2 \
    --metric total_amount  --scale total_amount=2 \
    --metric trip_distance --scale trip_distance=2 \
    --shard-by daily
```

Dollar amounts are then stored as integer **cents** (divide by 100 on read).
Rerun per month to load a wider range. Note `store_and_fwd_flag` /
`passenger_count` / `RatecodeID` are **nullable** in the TLC files and ingest
rejects nulls in declared columns — include those only after filtering them out.

## Schema (~18 columns)

| Column | Type | Notes |
|---|---|---|
| `VendorID` | int | Provider ID (1 = Creative Mobile, 2 = VeriFone) |
| `tpep_pickup_datetime` | timestamp | Trip start, second-resolution |
| `tpep_dropoff_datetime` | timestamp | Trip end |
| `passenger_count` | float (nullable) | |
| `trip_distance` | float | Miles |
| `RatecodeID` | float | Rate code (1 = standard, 2 = JFK, …) |
| `store_and_fwd_flag` | string | "Y" / "N" |
| `PULocationID` | int | Pickup taxi zone (joins to TLC taxi-zone lookup) |
| `DOLocationID` | int | Dropoff taxi zone |
| `payment_type` | int | 1 = credit card, 2 = cash, … |
| `fare_amount` | float | |
| `extra` | float | Surcharges |
| `mta_tax` | float | |
| `tip_amount` | float | |
| `tolls_amount` | float | |
| `improvement_surcharge` | float | |
| `total_amount` | float | |
| `congestion_surcharge` | float | |

## Why this dataset

- Real event records (one row per trip), not pre-aggregated metrics.
- Several low-/medium-cardinality dimensions: vendor, payment type,
  rate code (~5 each), pickup/dropoff zones (~265 each).
- A handful of numeric metrics suitable for sum / average / percentile.
- Used in canonical analytics-engine demos (ClickHouse, DuckDB, Trino).

## Licensing / attribution

Released as **NYC Open Data**. The TLC publishes these files for public use
under the terms at the landing page above. When deriving public artifacts
from this data, attribute the NYC Taxi & Limousine Commission.
