# Tutorial — a hands-on tour

Tutankhamun (`t9n`) is a Rust analytics engine for time-bucketed
columnar data. You ingest rows into immutable per-time-bucket
**shards** and run filter-and-aggregate queries against them.

In ten minutes this tutorial takes you from a fresh checkout to
querying ~56,000 real Citi Bike trips.

## Prerequisites

A recent Rust toolchain (`rustup` will pick the right one via
`rust-toolchain.toml`) and, for the real-data step, `curl`, `unzip`,
and `python3` on `PATH`.

## 1. Build it

```sh
cargo build --release --bin t9n
```

## 2. Get the data

The fetch script downloads a month of Jersey City Citi Bike trip data
and adds a `trip_seconds` column (computed from `ended_at - started_at`,
so there's an integer metric to aggregate). It leaves a ready-to-ingest
CSV — it does **not** ingest, so the next step stays yours:

```sh
scripts/fetch_citibike.sh
```

```
data ready: .cache/downloads/citibike/JC-202301-citibike-tripdata-with-duration.csv
```

## 3. Ingest it

Now you ingest. You tell `t9n` what each column *is*: the `--time`
column that drives bucketing, the int64 `--metric` columns you'll
aggregate, and the `--string` columns you'll filter on.
`--shard-by daily` writes one immutable shard per day:

```sh
cargo run --release --bin t9n -- ingest \
    .cache/downloads/citibike/JC-202301-citibike-tripdata-with-duration.csv \
    --output .cache/storage/citibike/jc-202301 \
    --time started_at \
    --metric trip_seconds \
    --string rideable_type --string member_casual \
    --string start_station_id --string end_station_id \
    --shard-by daily
```

```
wrote 56075 docs to .cache/storage/citibike/jc-202301
```

That column split *is* the storage model: metrics are summable but not
filterable; string fields are inverted-indexed for filtering but not
summable; the time column buckets rows into shards.

## 4. Look at what you have

The seed produces one shard per day, so the dataset is a directory
of 31 daily shards. Inspect one of them:

```sh
cargo run --release --bin t9n -- shard inspect \
    .cache/storage/citibike/jc-202301/2023-01-01
```

```
num docs:        ~1800
time range:      (2023-01-01 first trip .. last trip of that day)
schema:
  trip_seconds      metric  int64
  rideable_type     string  index
  member_casual     string  index
  start_station_id  string  index
  end_station_id    string  index
```

Each daily shard has the same schema; only the time range and per-shard
row counts vary.

## 5. Aggregate with the `query` verb

The `query` verb fans out across every shard under the directory and
sums the per-shard results. Total time spent riding in January:

```sh
cargo run --release --bin t9n -- query \
    .cache/storage/citibike/jc-202301 --metric trip_seconds
```

```
shards:   31 scanned
matched:  all 56075 docs
trip_seconds:   sum = 39679768
```

About 459 cumulative days of ride time in a month.

By rider type:

```sh
cargo run --release --bin t9n -- query \
    .cache/storage/citibike/jc-202301 \
    --filter member_casual=member --metric trip_seconds
# shards 31; matched 43642 / 56075; sum 24,776,477  (~9.5 min avg)

cargo run --release --bin t9n -- query \
    .cache/storage/citibike/jc-202301 \
    --filter member_casual=casual --metric trip_seconds
# shards 31; matched 12433 / 56075; sum 14,903,291  (~20 min avg)
```

Casuals are 22% of trips but 38% of ride time — they ride about
2× longer per trip.

`--aggregate min|max|avg` swaps `sum` for any of the others:

```sh
cargo run --release --bin t9n -- query \
    .cache/storage/citibike/jc-202301 \
    --metric trip_seconds --aggregate avg
# trip_seconds:   avg = 707.61   (~12 min per trip)
```

By starting station — Hoboken Terminal:

```sh
cargo run --release --bin t9n -- query \
    .cache/storage/citibike/jc-202301 \
    --filter start_station_id=HB101 --metric trip_seconds
# shards 31; matched 1734 / 56075; sum 1,467,974
```

`--filter` is repeatable — multiple clauses AND together. Members
starting at Hoboken Terminal:

```sh
cargo run --release --bin t9n -- query \
    .cache/storage/citibike/jc-202301 \
    --filter start_station_id=HB101 --filter member_casual=member \
    --metric trip_seconds
```

## 6. Query with SQL

`t9n sql` registers the dataset as a table named `t` and runs the query
through DataFusion. `WHERE` filters and single-column `GROUP BY` (with
`count` / `sum` / `min` / `max` / `approx_distinct`) push down into the
engine's scan where they can; anything else DataFusion computes over the
returned rows — either way you get the same answer.

Trips and total ride time by rider type:

```sh
cargo run --release --bin t9n -- sql .cache/storage/citibike/jc-202301 \
    "SELECT member_casual, count(*) AS trips, sum(trip_seconds) AS total
     FROM t GROUP BY member_casual ORDER BY member_casual"
```

```
+---------------+-------+----------+
| member_casual | trips | total    |
+---------------+-------+----------+
| casual        | 12433 | 14903291 |
| member        | 43642 | 24776477 |
+---------------+-------+----------+
```

`approx_distinct` is a HyperLogLog estimate — roughly how many distinct
start stations each rider type used, without keeping every value:

```sh
cargo run --release --bin t9n -- sql .cache/storage/citibike/jc-202301 \
    "SELECT member_casual, approx_distinct(start_station_id) AS stations
     FROM t GROUP BY member_casual ORDER BY member_casual"
```

```
+---------------+----------+
| member_casual | stations |
+---------------+----------+
| casual        |       81 |
| member        |       82 |
+---------------+----------+
```

Most popular bike type:

```sh
cargo run --release --bin t9n -- sql .cache/storage/citibike/jc-202301 \
    "SELECT rideable_type, count(*) AS trips
     FROM t GROUP BY rideable_type ORDER BY trips DESC"
```

```
+---------------+-------+
| rideable_type | trips |
+---------------+-------+
| classic_bike  | 43959 |
| electric_bike | 12016 |
| docked_bike   |   100 |
+---------------+-------+
```

Run `cargo run --release --bin t9n -- sql --help` for cache and
source-URL options.

## 7. Your own data

Run `cargo run --release --bin t9n -- ingest --help` to see the
full flag set. Time columns can be unix epoch seconds, RFC 3339,
or `YYYY-MM-DD HH:MM:SS`. Metric columns must be int64
(multiply decimals by 100 and round if needed).

For a dataset that's already split into multiple shards under a
directory, `t9n query <dir> --metric ... [--filter ...]` fans out
across all of them and prints one aggregate.

`t9n query`, `t9n sql`, and `t9n ingest --output` all accept an
object-storage URL — `s3://`, `gs://`, `az://`, or `file://` — instead
of a local path. On read, shards are downloaded into
`~/Library/Caches/tutankhamun` (or the platform equivalent) on
first read and reused from there on subsequent runs; override the
cache location with `--cache-dir` and its size with
`--cache-size 10GB` (or `50%` of disk). On ingest, shards are
staged locally and uploaded with `metadata.json` last per shard so
partial uploads stay invisible to readers.

## What's next

- [`.docs/tutankhamun.md`](.docs/tutankhamun.md) — the design,
  storage format, and where the production query path is going.
- [`.docs/roadmap.md`](.docs/roadmap.md) — what's shipped and
  what's coming.

Honesty notes for early adopters: the production wire surface — gRPC
sessions, Arrow Flight streaming, the Python client — isn't built yet.
The CLI verbs above (`ingest`, `query`, `sql`) are how you exercise the
engine today; `t9n sql`'s `GROUP BY` already runs through the FTGS
aggregation core.
