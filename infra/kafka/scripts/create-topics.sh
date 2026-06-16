#!/bin/bash
set -e

BROKER="kafka-1:19092"
REPLICATION=3

echo "[Kafka] Creating topics..."

/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BROKER --create --if-not-exists \
  --topic transactions --partitions 12 --replication-factor $REPLICATION
echo "[Kafka] Created: transactions (12 partitions)"

/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BROKER --create --if-not-exists \
  --topic rules-updates --partitions 1 --replication-factor $REPLICATION
echo "[Kafka] Created: rules-updates (1 partition)"

echo "[Kafka] Done. All topics created."
