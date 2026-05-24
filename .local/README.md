# `.local/` — local developer playground

A per-developer playground for experimenting with the `t9n` daemon against
real public data, **without leaving the project root** or provisioning
external infrastructure.

## What ships in git

Only scripts and READMEs. **No data ever goes into the repo.** See
[`.gitignore`](.gitignore) — the entire `data/`, `storage/`, `cache/`,
`state/` trees are ignored.

## Layout

```
.local/
├── seed/                   datasets to download + stage locally
│   ├── nyc-taxi/           NYC TLC yellow taxi trips (Parquet)
│   └── binance/            Binance 1-minute klines (CSV)
├── run/                    scripts that start / probe the daemon
│   ├── daemon.sh
│   └── storage-check.sh
├── storage/                gitignored — populated by fetch scripts;
│                           backs the daemon's `--storage-url file://…`
├── cache/                  gitignored — daemon's local hot-storage cache
├── data/                   gitignored — intermediate downloads if needed
└── state/                  gitignored — future runtime state
```

## Quick start

```sh
# 1. Build the daemon
cargo build --bin t9n

# 2. Fetch one or both datasets (each is idempotent; safe to re-run)
bash .local/seed/nyc-taxi/fetch.sh
bash .local/seed/binance/fetch.sh

# 3. Verify the daemon can see the staged data
bash .local/run/storage-check.sh                # list everything
bash .local/run/storage-check.sh nyc_taxi       # just NYC taxi
bash .local/run/storage-check.sh binance        # just Binance

# 4. Start the daemon against the local storage
bash .local/run/daemon.sh                       # foregrounded; Ctrl-C to stop
```

The daemon binds to `127.0.0.1:18080` for its operations HTTP port. Hit
`/healthz` and `/readyz` to confirm it's up:

```sh
curl http://127.0.0.1:18080/healthz             # → ok
curl http://127.0.0.1:18080/readyz              # → ready
```

## Datasets at a glance

| Dataset | Format | Resolution | Default volume | Source |
|---|---|---|---|---|
| NYC taxi yellow trips | Parquet | second-level pickup/dropoff timestamps | 3 months ≈ 150–200 MB | NYC TLC public Open Data |
| Binance 1m klines | CSV (in zip) | pre-bucketed at 1 minute | 2 symbols × 6 months ≈ 50–100 MB | Binance public market data |

See each dataset's `README.md` under `seed/` for schema, source URL, and
attribution.

## Today vs. tomorrow

What works today:
- Fetch scripts download and stage data under `storage/`
- The daemon starts with `--storage-url file://…/.local/storage` and can
  *see* the data via `t9n storage check`

What doesn't work yet:
- Querying the staged data — the data plane and ingest pipeline aren't
  written. Querying lands when those pieces of the roadmap do.

The fetch scripts won't need to change when ingest arrives; they just gain
an additional convert/load step.
