# Binance 1-minute klines

## Source

Binance public market data archive:

- Landing page: <https://github.com/binance/binance-public-data>
- Direct file URL (one zip per symbol-interval-month):
  `https://data.binance.vision/data/spot/monthly/klines/<SYMBOL>/<INTERVAL>/<SYMBOL>-<INTERVAL>-<YYYY-MM>.zip`

Each zip contains a single CSV.

## Usage

```sh
# Default: BTCUSDT + ETHUSDT, 2024-01..2024-06, 1-minute klines
bash .local/seed/binance/fetch.sh

# Add a third symbol and shorten the range
BINANCE_SYMBOLS="BTCUSDT ETHUSDT SOLUSDT" \
  BINANCE_MONTHS="2024-01 2024-02" \
  bash .local/seed/binance/fetch.sh

# Switch interval (5m, 15m, 1h, 1d are also published by Binance)
BINANCE_INTERVAL="5m" bash .local/seed/binance/fetch.sh
```

Downloaded to `.cache/downloads/binance/<SYMBOL>-<INTERVAL>-<YYYY-MM>.csv`
(raw source; ingestion turns these into shards under `.cache/storage/`).

## Schema (CSV, header-less, 12 columns)

| Col | Field | Type | Notes |
|---|---|---|---|
| 1 | `open_time` | int64 (ms) | Kline open time, Unix epoch ms |
| 2 | `open` | string (decimal) | |
| 3 | `high` | string (decimal) | |
| 4 | `low` | string (decimal) | |
| 5 | `close` | string (decimal) | |
| 6 | `volume` | string (decimal) | Base-asset volume |
| 7 | `close_time` | int64 (ms) | Open time + interval − 1 ms |
| 8 | `quote_asset_volume` | string (decimal) | |
| 9 | `number_of_trades` | int | |
| 10 | `taker_buy_base_volume` | string (decimal) | |
| 11 | `taker_buy_quote_volume` | string (decimal) | |
| 12 | `ignore` | int | Unused |

## Why this dataset

- Pre-bucketed at exactly minute resolution (or larger intervals if
  requested) — the canonical "time series with one high-cardinality
  dimension" shape.
- Symbol (filename-derived) is a natural high-cardinality grouping
  dimension when you stage many symbols.
- Numeric metrics (volume, trades, OHLC) are obvious sum / percentile
  targets.

## Licensing / attribution

Binance publishes this market data under the **Binance Terms of Use**
(<https://www.binance.com/en/terms>), which permits non-commercial /
research use. Do not redistribute the raw files in commercial products
without checking the terms.
