#!/bin/bash
set -e

BROKER="kafka-1:19092"
REPLICATION=3

echo "[Kafka] Creating topics..."

/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BROKER --create --if-not-exists \
  --topic transactions --partitions 10 --replication-factor $REPLICATION \
  --config min.insync.replicas=2
echo "[Kafka] Created: transactions (10 partitions)"

/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BROKER --create --if-not-exists \
  --topic rules-updates --partitions 1 --replication-factor $REPLICATION \
  --config min.insync.replicas=2
echo "[Kafka] Created: rules-updates (1 partition)"

/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BROKER --create --if-not-exists \
  --topic transactions-dlq --partitions 1 --replication-factor $REPLICATION \
  --config min.insync.replicas=2
echo "[Kafka] Created: transactions-dlq (1 partition)"

/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BROKER --create --if-not-exists \
  --topic rules-updates-dlq --partitions 1 --replication-factor $REPLICATION \
  --config min.insync.replicas=2
echo "[Kafka] Created: rules-updates-dlq (1 partition)"

echo "[Kafka] Done. All topics created."
