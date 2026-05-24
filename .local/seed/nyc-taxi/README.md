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

Staged under `.local/storage/nyc_taxi/yellow_tripdata_<YYYY-MM>.parquet`.

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
