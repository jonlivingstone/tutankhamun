# `.local/seed/` — playground data sources

One subdirectory per dataset. Each contains:

- `fetch.sh` — downloads the data and stages it under
  `.local/storage/<dataset>/…`. Idempotent (skips files that already exist).
- `README.md` — schema, source URL, attribution / license note.

Run them in any order; they don't depend on each other.

## Datasets

| Dir | Description | Default size |
|---|---|---|
| [`nyc-taxi/`](nyc-taxi/) | NYC TLC yellow taxi trips (Parquet, second-resolution events) | 3 months ≈ 150–200 MB |
| [`binance/`](binance/) | Binance 1-minute klines for BTCUSDT + ETHUSDT (CSV) | 6 months × 2 symbols ≈ 50–100 MB |

## Adding a new dataset

1. Create `.local/seed/<name>/` with `fetch.sh` and `README.md`.
2. `fetch.sh` should stage files under `.local/storage/<name>/…` so the
   prefix matches the directory name (this makes
   `storage-check.sh <name>` work).
3. Use the conventions shared by the existing scripts:
   - `#!/usr/bin/env bash` + `set -euo pipefail`
   - `curl --fail --location --silent --show-error` (`-fLsS`)
   - Skip-if-exists (re-running is a cheap noop)
   - Echo what's being done before doing it
