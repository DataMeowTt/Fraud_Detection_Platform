#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

fail() { echo "  ERROR: $1"; exit 1; }

echo "=== [1/2] Truncate ClickHouse ==="
(cd "$REPO_ROOT/services/transaction-generator" && python3 -m src.utils.clickhouse_utils) \
    || fail "truncate ClickHouse failed"
echo "  Done"

echo "=== [2/2] Stop infrastructure ==="
(cd "$REPO_ROOT/infra" && docker compose down) || fail "docker compose down failed"
echo "  Done"

echo ""
echo "Stop complete."
