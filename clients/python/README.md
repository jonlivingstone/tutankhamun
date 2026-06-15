# tutankhamun (Python client)

Native Python client for the [Tutankhamun](../../README.md) analytics engine. It
speaks Arrow FlightSQL over gRPC, holds a session, and composes SQL fragments
into a fluent, lazy API that returns zero-copy `pyarrow.Table` results.

## Install

```sh
pip install tutankhamun            # core (pyarrow)
pip install 'tutankhamun[polars]'  # + polars conversion
```

## Quickstart

Start a daemon (default gRPC port `50051`; see the repo README for `t9n serve`),
then:

```python
import tutankhamun as tk

conn = tk.connect("grpc://localhost:50051")

with conn.session(dataset="nyc_taxi") as s:
    # A defined metric is a named SQL expression you can reuse in fragments.
    # (Dollar columns are stored as integer cents — divide by 100 on read.)
    s.define("extras", "total_amount - fare_amount")   # tips + tolls + surcharges

    table = (
        s.filter("tpep_pickup_datetime >= TIMESTAMP '2024-01-01'")   # year 2024
         .filter("tpep_pickup_datetime <  TIMESTAMP '2025-01-01'")
         .group_by("date_trunc('month', tpep_pickup_datetime)")
         .select("date_trunc('month', tpep_pickup_datetime) AS month, "
                 "count(*) AS trips, sum(fare_amount) AS fare_cents, "
                 "sum(extras) AS extras_cents")
         .fetch()                        # -> pyarrow.Table
    )

    df = table.to_pandas()               # native; or tk.to_polars(table)
    print(df)
```

`Query` is immutable — each `.filter()` / `.group_by()` / `.select()` returns a
new query, so you can branch a base off into several drill-downs:

```python
paid       = s.filter("fare_amount > 0")
by_payment = paid.group_by("payment_type").select("payment_type, count(*) AS n").fetch()
card       = paid.filter("payment_type = 1").group_by("VendorID").select("count(*)").fetch()
```

For a base relation that isn't a plain dataset, use `session_from_sql`:

```python
with conn.session_from_sql("SELECT * FROM nyc_taxi WHERE fare_amount > 0") as s:
    ...
```

Arbitrary SQL (including DDL like `CREATE VIEW`) runs via `s.sql("...")`.

## Notes

- **Sessions.** Opening a session reserves a server-side memory budget and mints
  a token. The session is reclaimed after 30 min idle (the clock resets on each
  query) or 4 h; `with` / `.close()` frees it immediately. If a session is lost,
  the client transparently reopens and replays your `define`s.
- **Not yet:** `time_range=` on `session()` (use `session_from_sql` meanwhile),
  `to_duckdb()`, prepared statements.

## Development

```sh
pip install -e '.[dev,polars]'
pytest                 # unit tests (no daemon)
# integration tests run if a daemon is reachable:
TUT_TEST_ADDR=grpc://localhost:50051 pytest
```
