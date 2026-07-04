#!/bin/bash

FLINK_URL="http://localhost:8081"
JAR_PATH="../flink-jobs/decision-aggregator/target/decision-aggregator-1.0.0.jar"
KAFKA="kafka-1"
KAFKA_BIN="/opt/kafka/bin"

fail() { echo "  ERROR: $1"; exit 1; }

echo "=== [1/6] Cancel Flink job ==="
JOB_ID=$(curl -sf "$FLINK_URL/jobs" | python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
running = [j['id'] for j in jobs if j['status'] == 'RUNNING']
print(running[0] if running else '')
" 2>/dev/null)
if [ -n "$JOB_ID" ]; then
    curl -sf -X PATCH "$FLINK_URL/jobs/$JOB_ID?mode=cancel" > /dev/null
    echo "  Cancelled: $JOB_ID"
    sleep 5
else
    echo "  No running job"
fi

echo "=== [2/6] Truncate ClickHouse ==="
docker exec clickhouse-server clickhouse-client \
    --query "TRUNCATE TABLE fraud_detection.transactions" \
    || fail "truncate transactions"
docker exec clickhouse-server clickhouse-client \
    --query "TRUNCATE TABLE fraud_detection.ground_true_transactions" \
    || fail "truncate ground_true_transactions"
echo "  Done"

echo "=== [3/6] Reset Kafka topics ==="
docker exec $KAFKA $KAFKA_BIN/kafka-topics.sh \
    --bootstrap-server $KAFKA:19092 --delete --topic transactions 2>/dev/null || true
docker exec $KAFKA $KAFKA_BIN/kafka-topics.sh \
    --bootstrap-server $KAFKA:19092 --delete --topic rules-updates 2>/dev/null || true
sleep 3
docker exec $KAFKA $KAFKA_BIN/kafka-topics.sh \
    --bootstrap-server $KAFKA:19092 \
    --create --topic transactions --partitions 10 --replication-factor 2 \
    || fail "create topic transactions"
docker exec $KAFKA $KAFKA_BIN/kafka-topics.sh \
    --bootstrap-server $KAFKA:19092 \
    --create --topic rules-updates --partitions 1 --replication-factor 2 \
    || fail "create topic rules-updates"
echo "  Done"

echo "=== [4/6] Clear Flink checkpoints ==="
docker exec minio mc rm --recursive --force /data/flink-checkpoints/ 2>/dev/null
echo "  Done"

echo "=== [5/6] Build Flink job ==="
(cd ../flink-jobs && mvn clean package -pl decision-aggregator -am -DskipTests -q) \
    || fail "mvn build failed"
echo "  Done"

echo "=== [6/6] Submit Flink job ==="
[ -f "$JAR_PATH" ] || fail "JAR not found: $JAR_PATH"
UPLOAD=$(curl -sf -X POST "$FLINK_URL/jars/upload" \
    -H "Expect:" \
    -F "jarfile=@$JAR_PATH") || fail "upload JAR"
JAR_ID=$(echo "$UPLOAD" | python3 -c "import sys,json; print(json.load(sys.stdin)['filename'].split('/')[-1])")
curl -sf -X POST "$FLINK_URL/jars/$JAR_ID/run" \
    -H "Content-Type: application/json" \
    -d '{}' > /dev/null || fail "submit job"
echo "  Submitted: $JAR_ID"

echo ""
echo "Reset complete."
