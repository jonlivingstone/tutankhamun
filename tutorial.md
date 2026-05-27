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

## 2. Ingest real data

The bundled seed script downloads a month of Jersey City Citi Bike
trip data, adds a `trip_seconds` column (computed from
`ended_at - started_at` so there's an integer metric to sum
against), and ingests it as a shard:

```sh
scripts/seed_citibike.sh
```

```
shard ready at: .local/storage/citibike/jc-202301
```

## 3. Look at what you have

```sh
cargo run --release --bin t9n -- shard inspect \
    .local/storage/citibike/jc-202301
```

```
num docs:        56075
time range:      1672531596 .. 1675209491
                 (2023-01-01T00:06:36+00:00 .. 2023-01-31T23:58:11+00:00)
schema:
  trip_seconds      metric  int64
  rideable_type     string  index (3 terms)
  member_casual     string  index (2 terms)
  start_station_id  string  index (82 terms)
  end_station_id    string  index (122 terms)
```

One month, 56k trips, four categorical fields, one sum-able metric.

## 4. Ask questions

Total time spent riding in January:

```sh
cargo run --release --bin t9n -- shard query \
    .local/storage/citibike/jc-202301 --metric trip_seconds
```

```
matched:  all 56075 docs
trip_seconds:   sum = 39679768
```

About 459 cumulative days of ride time in a month.

By rider type:

```sh
cargo run --release --bin t9n -- shard query \
    .local/storage/citibike/jc-202301 \
    --filter member_casual=member --metric trip_seconds
# matched 43642 / 56075; sum 24,776,477  (~9.5 min avg)

cargo run --release --bin t9n -- shard query \
    .local/storage/citibike/jc-202301 \
    --filter member_casual=casual --metric trip_seconds
# matched 12433 / 56075; sum 14,903,291  (~20 min avg)
```

Casuals are 22% of trips but 38% of ride time — they ride about
2× longer per trip.

By starting station — Hoboken Terminal:

```sh
cargo run --release --bin t9n -- shard query \
    .local/storage/citibike/jc-202301 \
    --filter start_station_id=HB101 --metric trip_seconds
# matched 1734 / 56075; sum 1,467,974
```

## 5. Your own data

Run `cargo run --release --bin t9n -- ingest --help` to see the
full flag set. Time columns can be unix epoch seconds, RFC 3339,
or `YYYY-MM-DD HH:MM:SS`. Metric columns must be int64
(multiply decimals by 100 and round if needed).

For a dataset that's already split into multiple shards under a
directory, `t9n query <dir> --metric ... [--filter ...]` fans out
across all of them and prints one aggregate.

## What's next

- [`.docs/tutankhamun.md`](.docs/tutankhamun.md) — the design,
  storage format, and where the production query path is going.
- [`.docs/roadmap.md`](.docs/roadmap.md) — what's shipped and
  what's coming.

Honesty notes for early adopters: the production query surface
(gRPC, sessions, FTGS, Python client) isn't built yet. The CLI
verbs above are how you exercise the storage and query format
today.
