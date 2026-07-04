# Data Flow

This document traces a single transaction — and a single rule update — end to end through the
platform. For component-level design, see `component-design.md`.

## Transaction Lifecycle

1. **Production**: `services/transaction-generator` builds a `Transaction` record (see
   `schemas/transaction-schema.json`), stamps `produced_at` with the current wall-clock time, and
   publishes it to the Kafka `transactions` topic (10 partitions), keyed by `account_id`.

2. **Ingestion**: `DecisionAggregatorJob` consumes `transactions` via a `KafkaSource` with a
   bounded-out-of-orderness watermark strategy (5s), using `event_time` as the event-time field,
   and keys the stream by `account_id`.

3. **Rule check**: `DecisionPipeline.processElement()` first calls `RuleEvaluator.evaluate()`
   against the current broadcast rule set. If a rule matches, the transaction is immediately
   decided (`BLOCK` or `ALERT`) and flows straight to the sinks — CEP and ML are skipped.

4. **Priority-account bypass**: if the account is in the `PRIORITY_ACCOUNTS` allow-list (used for
   controlled testing scenarios), the transaction is auto-approved and also skips CEP/ML.

5. **CEP check**: if no rule matched, `CepEvaluator.evaluate()` runs the four sequence-based
   patterns (see `component-design.md`) against per-account keyed state. A match again short-
   circuits straight to the sinks.

6. **ML scoring**: if both prior stages pass, `FeatureExtractor.extract()` computes the nine-value
   feature vector from rolling per-account state, and the transaction is placed in a pending
   batch. The batch is scored by `FraudScorer.scoreBatch()` once it reaches `MAX_BATCH_SIZE`
   (1024) or `MAX_WAIT_MS` (30ms) elapses, whichever comes first (a processing-time timer drives
   the timeout path). The resulting score is compared against the fraud threshold to produce
   `ALERT` or `APPROVED`.

7. **Decision persistence**: every decision, regardless of which stage produced it, is written to
   a `FraudDecision` record with `producedAt` (copied from the source transaction) and
   `decidedAt` (stamped at decision time) and passed to `ClickHouseSink`. The sink batches records
   in a bounded queue and flushes to `fraud_detection.transactions` on whichever comes first:
   5,000 buffered rows or a 2s max-wait timer.

8. **Alerting**: `FraudAlertSink` is designed to publish every non-`APPROVED` decision to the
   `fraud-alerts` Kafka topic for downstream consumers. It is currently implemented but not
   attached to the job's output stream (see `component-design.md`).

## Latency Measurement

Every `FraudDecision` carries both `producedAt` (when the source transaction entered Kafka) and
`decidedAt` (when `DecisionPipeline` reached a verdict). End-to-end latency is computed in
ClickHouse as `dateDiff('millisecond', produced_at, decided_at)`, aggregated as needed (avg,
p50/p95/p99). This is the same query pattern used by
`services/transaction-generator/src/utils/latency.py` and `benchmarks/metrics_collector.py`.

## Rule Update Propagation

Rule updates follow a separate, simpler path that intersects the transaction flow only through
broadcast state:

1. A `FraudRule` JSON payload is published to the `rules-updates` Kafka topic (1 partition).
2. `DecisionAggregatorJob` consumes it via a second `KafkaSource`, broadcast to all parallel
   subtasks using `RuleStateDescriptor.DESCRIPTOR`.
3. `DecisionPipeline.processBroadcastElement()` updates the shared `BroadcastState`: enabled
   rules are stored under their `rule_id`; disabled rules are removed.
4. Every subsequent transaction evaluated by any subtask — regardless of which task manager
   received the rule update — sees the new rule immediately, with no job restart.

This propagation delay (rule produced → rule enforced) is what
`flink-jobs/rule-engine/src/test/java/com/frauddetection/rules/RuleEngineTest.java` measures: it
records the timestamp at which a `BLOCK` rule is acknowledged by Kafka, sends a burst of
transactions for the targeted account, and measures the time until the first `BLOCK` decision for
that account appears in ClickHouse.

## Offline ML Training Flow (separate from the online path)

The model scored by `FraudScorer` at runtime is trained offline and is not part of the streaming
data flow:

1. `services/ml-training/notebooks/generate.ipynb` synthesizes a labelled transaction dataset.
2. `training.ipynb` performs feature engineering (the same nine features as `FeatureExtractor`),
   tunes and trains an XGBoost classifier, and saves it as `model_detection.json`.
3. `export_ml.py` is intended to upload the trained model to the `ml-models` MinIO bucket under
   the key `FraudScorer` expects (`MINIO_MODEL_KEY`, default `model_detection.json`) — this script
   currently points at a mismatched filename/key and needs to be corrected before relying on it.
4. `FraudScorer.open()` downloads and loads this model once when the Flink job starts.
