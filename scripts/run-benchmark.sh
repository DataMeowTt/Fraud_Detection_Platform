#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

echo "[Benchmark] Running load test..."
python3 "$REPO_ROOT/benchmarks/load_test.py" "$@"

echo "[Benchmark] Collecting metrics..."
python3 "$REPO_ROOT/benchmarks/metrics_collector.py"
