# System Overview

## Purpose

This platform is a real-time fraud detection system for banking transactions. It ingests a
continuous transaction stream, evaluates each transaction against three independent detection
engines, and produces an `APPROVED` / `ALERT` / `BLOCK` decision within sub-second latency. The
platform is built as a learning exercise in stream processing with Apache Flink, and its
functional scope, performance targets, and evaluation criteria are defined in `TASK.md` at the
repository root.

## High-Level Architecture

```
                +------------------+
                | Transaction Gen  |
                +------------------+
                          |
                          v
                +------------------+
                | Kafka Topic      |
                | transactions     |
                +------------------+
                          |
                          v
                +------------------+
                | Apache Flink     |
                | Streaming Engine |
                +------------------+
                   |      |      |
                   |      |      +--> ML Scoring
                   |      +---------> CEP Detection
                   +----------------> Rules Engine
                          |
                          v
                +------------------+
                | Fraud Decision   |
                +------------------+
                          |
          +---------------+----------------+
          |                                |
          v                                v
+------------------+          +----------------------+
| ClickHouse       |          | Alert Topic          |
| Analytics DB     |          | Fraud Alerts         |
+------------------+          +----------------------+
          |
          v
+------------------+
| Grafana          |
+------------------+
```

## Core Components

| Component | Technology | Responsibility |
|---|---|---|
| Transaction Generator | Python (`services/transaction-generator`) | Produces synthetic transactions and rule updates to Kafka, with labelled fraud scenarios for evaluation |
| Message Bus | Apache Kafka (3-broker cluster) | Decouples ingestion from processing; hosts `transactions`, `rules-updates`, and `fraud-alerts` topics |
| Stream Processing | Apache Flink 1.20 (`flink-jobs/`) | Runs the single `DecisionAggregatorJob`, which composes the Rules, CEP, and ML engines |
| Object Storage | MinIO | Stores the trained XGBoost model (`ml-models` bucket) and Flink checkpoints/savepoints |
| Analytics Store | ClickHouse | Stores every decided transaction for latency analysis, dashboards, and offline evaluation |
| Monitoring | Prometheus + Grafana | Scrapes Flink JobManager/TaskManager metrics; ClickHouse-backed dashboards for business metrics |
| Offline ML Training | Jupyter notebooks (`services/ml-training`) | Generates training data, trains the XGBoost model, and exports it to MinIO |

## Flink Job Topology

The platform runs a single Flink job, `DecisionAggregatorJob`
(`flink-jobs/decision-aggregator/src/main/java/com/frauddetection/aggregator/DecisionAggregatorJob.java`),
rather than three independent jobs. It reads the `transactions` topic keyed by `account_id`,
connects it to a broadcast stream of `rules-updates`, and evaluates each transaction through
`DecisionPipeline` — a single `KeyedBroadcastProcessFunction` that internally calls into the
Rules, CEP, and ML engines in priority order. See `component-design.md` for engine-level detail
and `data-flow.md` for the full request-to-decision trace.

## Key Design Decisions

- **Broadcast State for Rules**: new fraud rules are pushed through the `rules-updates` topic and
  broadcast to all parallel subtasks without requiring a job restart (Flink Broadcast State
  Pattern). This is the mechanism validated by the rule-update latency test in
  `flink-jobs/rule-engine/src/test/java/com/frauddetection/rules/RuleEngineTest.java`.
- **Single Model Load**: the XGBoost model is downloaded from MinIO and loaded into memory once,
  in `FraudScorer.open()`, rather than per record.
- **Hierarchical Decision Engine**: Rules take precedence over CEP, which takes precedence over
  ML scoring. A transaction only reaches the (comparatively expensive) ML stage if it clears both
  the Rules and CEP checks. See `component-design.md` for the exact ordering.
- **Mini-Batch ML Scoring**: transactions pending an ML score are accumulated into a small batch
  (bounded by count and a short max-wait timer) before a single XGBoost `DMatrix` inference call,
  instead of scoring one record at a time, to amortize JNI call overhead.
- **Checkpointing over Exactly-Once**: the job checkpoints every 600s in `AT_LEAST_ONCE` mode;
  ClickHouse's `ReplacingMergeTree` engine (keyed on `transaction_id`) absorbs duplicate writes on
  recovery.

## Deployment

The full stack (Kafka, Flink JobManager/TaskManager, ClickHouse, MinIO, Prometheus, Grafana) is
defined in `infra/docker-compose.yml` and orchestrated via the helper scripts in `scripts/` and
`infra/reset.sh`. See `docs/runbooks/local-setup.md` for step-by-step instructions.
