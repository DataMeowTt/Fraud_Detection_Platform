# Component Design

This document describes the design of each engine inside `DecisionAggregatorJob`, in the order
they are evaluated. For the overall topology, see `system-overview.md`; for the full request
trace, see `data-flow.md`.

## Common Module (`flink-jobs/common`)

Shared models and (de)serializers used by every other module:

- **Models** (`common/model`): `Transaction`, `FraudRule`, `FraudDecision`, `DecisionStatus`
  (`APPROVED` / `ALERT` / `BLOCK`). All are plain `Serializable` POJOs with Jackson
  `@JsonProperty` annotations mapping to the snake_case wire format defined in
  `schemas/transaction-schema.json`.
- **Serialization** (`common/serialization`): `TransactionDeserializer` and
  `FraudRuleDeserializer` implement Flink's `DeserializationSchema` to parse the raw JSON bytes
  read from Kafka into the corresponding model class.

## Rules Engine (`flink-jobs/rule-engine`)

**Responsibility**: evaluate hard, operator-defined thresholds (e.g. "block any transaction over
1,000,000,000 VND") and account-level blacklists, with rules updatable at runtime.

- `RuleEvaluator` (`rules/processor/RuleEvaluator.java`) is a stateless evaluator: given a
  transaction and the current set of enabled rules, it checks each rule's `field`
  (`amount` or `account_id`) against its `operator` (`GT`, `GTE`, `LT`, `LTE`, `EQ`, `NEQ`) and
  `threshold`/`value`. The first matching enabled rule determines the outcome (`BLOCK` or
  `ALERT`, per the rule's `action`).
- `RuleStateDescriptor` defines the `MapStateDescriptor<String, FraudRule>` broadcast to every
  parallel subtask. New rules arrive on the `rules-updates` Kafka topic and are merged into this
  broadcast state by `DecisionPipeline.processBroadcastElement()` — disabled rules are removed
  from state rather than merely flagged, keeping the active rule set small.
- Rules carry no static configuration; behavior is entirely driven by the broadcast rule set, so
  there is no equivalent to `cep-engine.properties` for this engine.

## CEP Engine (`flink-jobs/cep-engine`)

**Responsibility**: detect suspicious *sequences* of transactions for a given account within a
time window, using Flink's keyed state rather than the Flink CEP library directly.

`CepEvaluator` (`cep/processor/CepEvaluator.java`) runs four patterns, checked in this order
(block patterns first):

| Pattern | Type | Condition |
|---|---|---|
| `LOCATION_JUMP` | BLOCK | 2+ distinct transaction locations for the same account within a 1h window |
| `HIGH_FREQUENCY` | BLOCK | 3+ approved transactions over 100,000,000 VND within a 1h window |
| `DECLINED_BURST` | ALERT | 3+ `Declined` transactions within a 120s window |
| `RAPID_MICROPAYMENTS` | ALERT | 3+ transactions in the [2,000, 10,000] VND range within a 120s window |

Thresholds are configurable via `cep-engine.properties`, loaded once per operator instance through
`CepEvaluator.loadParams()`. `WindowedCountPattern` is the shared base class for the three
count-based patterns; `LocationJumpPattern` implements its own distinct-location tracking. All
per-account history is kept in Flink `ListState` with a TTL slightly longer than the pattern's
window, so state does not grow unbounded.

## ML Scoring Engine (`flink-jobs/ml-scoring`)

**Responsibility**: score transactions that clear both the Rules and CEP checks using a
pre-trained XGBoost model, producing a probability used to decide `ALERT` vs `APPROVED`.

- `FeatureExtractor` (`ml/features/FeatureExtractor.java`) maintains a rolling 24h transaction
  history per account (`ListState`) and computes nine features per transaction: `amount`,
  `hourOfDay`, `isHome`, `isDomestic`, `avg24h`, `logAmountRatio`, `count1h`, `count3h`,
  `timeSince`. This feature order must match the model's training-time feature order exactly
  (verified against `services/ml-training/notebooks/training.ipynb`).
- `ModelLoader` (`ml/loader/ModelLoader.java`) downloads the serialized model bytes from the
  `ml-models` MinIO bucket.
- `FraudScorer` (`ml/scorer/FraudScorer.java`) loads the XGBoost `Booster` once in `open()` and
  exposes `scoreBatch(float[][])`, which builds a single `DMatrix` for the whole batch rather than
  one `DMatrix` per record — this was a measured latency fix (see `component-design.md`'s sibling
  performance notes / prior latency investigation) to amortize XGBoost4j's JNI call overhead.
- The fraud threshold (default `0.9184`) and MinIO connection details are currently read from
  environment variables with hardcoded fallbacks in `FraudScorer.open()`; the accompanying
  `ml-scoring.properties` resource file is not currently loaded by any code path.

## Decision Engine (`flink-jobs/decision-aggregator`)

**Responsibility**: orchestrate the three engines above into a single decision per transaction,
and persist the result.

- `DecisionAggregatorJob` wires the Kafka sources, broadcast connect, and sinks (see
  `data-flow.md`).
- `DecisionPipeline` (`aggregator/pipeline/DecisionPipeline.java`) is the
  `KeyedBroadcastProcessFunction` implementing the priority order: **Rules → CEP → ML**. An
  explicit `PRIORITY_ACCOUNTS` allow-list bypasses CEP/ML entirely (used for the rule-update
  latency test scenario). Transactions pending an ML score are buffered and scored in batches
  bounded by `MAX_BATCH_SIZE` (1024) or `MAX_WAIT_MS` (30ms), whichever comes first.
- `ClickHouseSink` (`aggregator/sink/ClickHouseSink.java`) batches decisions into ClickHouse
  inserts, flushing on whichever comes first: 5,000 buffered rows or a 2s max-wait timer, via a
  dedicated background flusher thread.
- `FraudAlertSink` (`aggregator/sink/FraudAlertSink.java`) publishes non-`APPROVED` decisions to
  the `fraud-alerts` Kafka topic. **Note**: this sink is implemented but not currently attached to
  the job's output stream in `DecisionAggregatorJob.main()` — wiring it in is a known follow-up
  item.
