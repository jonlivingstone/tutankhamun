#!/usr/bin/env bash
# Start the t9n daemon with playground-friendly settings:
#   - ops port bound to 127.0.0.1:18080 (off the default 0.0.0.0:8080 to
#     avoid clashing with other dev services)
#   - storage URL pointing at .cache/storage (the object store — the daemon's
#     read-only source of ingested shards)
#   - cache directory under .cache/cache (the daemon's local mmap working copy)
#
# Overridable via env:
#   OPS_ADDR  default: 127.0.0.1:18080
#   RUST_LOG  default: info

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
STORAGE="$PROJECT_ROOT/.cache/storage"
CACHE="$PROJECT_ROOT/.cache/cache"
BINARY="$PROJECT_ROOT/target/debug/t9n"

if [[ ! -x "$BINARY" ]]; then
  echo "error: $BINARY not found. Run 'cargo build --bin t9n' first." >&2
  exit 1
fi

mkdir -p "$STORAGE" "$CACHE"

export RUST_LOG="${RUST_LOG:-info}"
OPS_ADDR="${OPS_ADDR:-127.0.0.1:18080}"

echo "starting t9n daemon"
echo "  binary    : $BINARY"
echo "  ops addr  : $OPS_ADDR"
echo "  storage   : file://$STORAGE"
echo "  cache     : $CACHE"
echo

exec "$BINARY" serve \
  --ops-addr "$OPS_ADDR" \
  --storage-url "file://$STORAGE" \
  --cache-dir "$CACHE"
