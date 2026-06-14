# `.local/` — local developer playground

A per-developer playground for experimenting with the `t9n` daemon against
real public data, **without leaving the project root** or provisioning
external infrastructure.

## What ships in git

Only scripts and READMEs. **No data ever goes into the repo.** All
generated state — raw downloads, the object store, and the daemon's cache —
lives under `.cache/` at the repo root, gitignored wholesale. Reset the
whole playground with a single **`rm -rf .cache`**.

## Layout

`.local/` holds only committed scripts; all generated state lives under
`.cache/`.

```
.local/                     committed scripts only
├── seed/                   datasets to download
│   ├── nyc-taxi/           NYC TLC yellow taxi trips (Parquet)
│   └── binance/            Binance 1-minute klines (CSV)
└── run/                    scripts that start / probe the daemon
    ├── daemon.sh
    └── storage-check.sh

.cache/                     generated state — gitignored, `rm -rf .cache` to reset
├── downloads/<dataset>/    raw fetched source (fetch scripts write here)
├── storage/<dataset>/      ingested shards — daemon `--storage-url file://…`
└── cache/                  daemon's local mmap working copy (`--cache-dir`)
```

## Quick start

```sh
# 1. Build the daemon
cargo build --bin t9n

# 2. Fetch raw source data into .cache/downloads/ (idempotent)
bash .local/seed/nyc-taxi/fetch.sh
bash .local/seed/binance/fetch.sh

# 3. Ingest it into the object store (.cache/storage/). The seeds are
#    download-only, so this step is manual — see each dataset's README,
#    and ../tutorial.md for a full worked example (Citi Bike).

# 4. Verify the daemon can see the ingested shards
bash .local/run/storage-check.sh                # list everything
bash .local/run/storage-check.sh nyc_taxi       # just NYC taxi

# 5. Start the daemon against .cache/storage
bash .local/run/daemon.sh                       # foregrounded; Ctrl-C to stop
```

The daemon binds to `127.0.0.1:18080` for its operations HTTP port. Hit
`/healthz` and `/readyz` to confirm it's up:

```sh
curl http://127.0.0.1:18080/healthz             # → ok
curl http://127.0.0.1:18080/readyz              # → ready
```

Or open `http://127.0.0.1:18080/status` in a browser for the live status
page (build, uptime, memory, sessions, datasets, recent queries).

## Datasets at a glance

| Dataset | Format | Resolution | Default volume | Source |
|---|---|---|---|---|
| NYC taxi yellow trips | Parquet | second-level pickup/dropoff timestamps | 3 months ≈ 150–200 MB | NYC TLC public Open Data |
| Binance 1m klines | CSV (in zip) | pre-bucketed at 1 minute | 2 symbols × 6 months ≈ 50–100 MB | Binance public market data |

See each dataset's `README.md` under `seed/` for schema, source URL, and
attribution.

## Fetch → ingest → query

The seed `fetch.sh` scripts are **download-only** — they populate
`.cache/downloads/<dataset>/`. To make a dataset queryable you ingest it
into the object store with `t9n ingest`, which writes shards +
`metadata.json` under `.cache/storage/<dataset>/` (see each dataset's
README; NYC taxi / Binance need a convert + cast step first). The Citi
Bike path in [`../tutorial.md`](../tutorial.md) walks the full
fetch → ingest → query flow end to end.

Once ingested, the daemon serves it: `daemon.sh` runs `t9n serve` against
`.cache/storage`, and the `ingest`, `query`, `sql`, and FlightSQL paths all
work today — browse/query over a SQL client or watch `/status`.
