#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
FLINK_URL="http://localhost:8081"
JAR_PATH="$REPO_ROOT/flink-jobs/decision-aggregator/target/decision-aggregator-1.0.0.jar"

fail() { echo "  ERROR: $1"; exit 1; }

echo "=== [1/3] Start infrastructure ==="
(cd "$REPO_ROOT/infra" && docker compose up -d) || fail "docker compose up failed"

echo "=== [2/3] Build Flink job ==="
(cd "$REPO_ROOT/flink-jobs" && mvn clean package -pl decision-aggregator -am -DskipTests -q) \
    || fail "mvn build failed"
echo "  Done"

echo "=== [3/3] Submit Flink job ==="
[ -f "$JAR_PATH" ] || fail "JAR not found: $JAR_PATH"

echo "  Waiting for Flink REST API..."
until curl -sf "$FLINK_URL/overview" > /dev/null 2>&1; do
    sleep 2
done

UPLOAD=$(curl -sf -X POST "$FLINK_URL/jars/upload" \
    -H "Expect:" \
    -F "jarfile=@$JAR_PATH") || fail "upload JAR"
JAR_ID=$(echo "$UPLOAD" | python3 -c "import sys,json; print(json.load(sys.stdin)['filename'].split('/')[-1])")
curl -sf -X POST "$FLINK_URL/jars/$JAR_ID/run" \
    -H "Content-Type: application/json" \
    -d '{}' > /dev/null || fail "submit job"
echo "  Submitted: $JAR_ID"

echo ""
echo "Start complete. Flink UI: $FLINK_URL"
