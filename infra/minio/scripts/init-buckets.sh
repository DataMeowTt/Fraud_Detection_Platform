#!/bin/bash
set -e

MINIO_ALIAS="local"
MINIO_URL="http://minio:9000"

echo "[MinIO Init] Waiting for MinIO to be ready..."
until mc alias set "$MINIO_ALIAS" "$MINIO_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" > /dev/null 2>&1; do
  echo "[MinIO Init] MinIO not ready yet, retrying in 2s..."
  sleep 2
done

echo "[MinIO Init] MinIO is ready. Creating buckets..."

mc mb --ignore-existing "$MINIO_ALIAS/flink-checkpoints"
echo "[MinIO Init] Created: flink-checkpoints"

mc mb --ignore-existing "$MINIO_ALIAS/flink-savepoints"
echo "[MinIO Init] Created: flink-savepoints"

mc mb --ignore-existing "$MINIO_ALIAS/ml-models"
echo "[MinIO Init] Created: ml-models"

echo "[MinIO Init] Done. All buckets are ready."
mc ls "$MINIO_ALIAS"
