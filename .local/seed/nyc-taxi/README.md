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

Records are Parquet with **float** dollar/distance columns, but `t9n ingest`
reads CSV/TSV and stores **int64** metrics — so convert + cast first (needs
[duckdb](https://duckdb.org); `brew install duckdb`):

```sh
duckdb -c "COPY (
  SELECT tpep_pickup_datetime                AS pickup_ts,
         VendorID                            AS vendor_id,
         PULocationID                        AS pu_zone,
         DOLocationID                        AS do_zone,
         payment_type,
         store_and_fwd_flag,
         CAST(ROUND(fare_amount   * 100) AS BIGINT) AS fare_cents,
         CAST(ROUND(tip_amount    * 100) AS BIGINT) AS tip_cents,
         CAST(ROUND(total_amount  * 100) AS BIGINT) AS total_cents,
         CAST(ROUND(trip_distance * 100) AS BIGINT) AS dist_centimiles
  FROM '.cache/downloads/nyc_taxi/yellow_tripdata_2024-01.parquet'
) TO '.cache/downloads/nyc_taxi/2024-01.csv' (HEADER)"

cargo run --release --bin t9n -- ingest \
    .cache/downloads/nyc_taxi/2024-01.csv \
    --output .cache/storage/nyc_taxi \
    --time pickup_ts \
    --int vendor_id --int pu_zone --int do_zone --int payment_type \
    --string store_and_fwd_flag \
    --metric fare_cents --metric tip_cents --metric total_cents \
    --metric dist_centimiles \
    --shard-by daily
```

Dollar amounts are stored as integer **cents** and distance as **centimiles**
(divide by 100 on read). `--shard-by daily` writes one shard per day; rerun
the convert+ingest per month to load a wider range.

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
