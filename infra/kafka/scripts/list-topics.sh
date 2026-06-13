#!/bin/bash

BROKER="kafka-1:19092"

echo "[Kafka] Listing all topics:"
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BROKER \
  --list

echo ""
echo "[Kafka] Topic details:"
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BROKER \
  --describe \
  --topics-with-overrides
