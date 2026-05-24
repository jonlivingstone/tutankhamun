#!/usr/bin/env bash
# Smoke-test the playground storage: list objects under .local/storage,
# optionally filtered to a prefix.
#
# Usage:
#   bash .local/run/storage-check.sh              # everything
#   bash .local/run/storage-check.sh nyc_taxi     # just the NYC taxi prefix
#   bash .local/run/storage-check.sh binance      # just the Binance prefix

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
STORAGE="$PROJECT_ROOT/.local/storage"
BINARY="$PROJECT_ROOT/target/debug/t9n"

if [[ ! -x "$BINARY" ]]; then
  echo "error: $BINARY not found. Run 'cargo build --bin t9n' first." >&2
  exit 1
fi

mkdir -p "$STORAGE"

if [[ $# -ge 1 ]]; then
  exec "$BINARY" storage check --url "file://$STORAGE" --prefix "$1"
else
  exec "$BINARY" storage check --url "file://$STORAGE"
fi
