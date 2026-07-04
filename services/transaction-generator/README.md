# Transaction Generator

Synthetic transaction and rule-update producer for the fraud detection platform. It publishes a
continuous, configurable-throughput stream to Kafka, seeded with a mix of normal traffic and
labelled fraud scenarios, so the Flink pipeline and its evaluation tooling have realistic,
ground-truth-tagged data to run against.

## What It Does

- Publishes transactions to the Kafka `transactions` topic and rule updates to `rules-updates`
  (`src/producer.py`), matching the wire format in `schemas/transaction-schema.json`.
- Drives throughput second-by-second from a configurable base TPS, following a 24h cycle of
  time-of-day windows (`src/generator/transaction_generator.py`), and supports live TPS changes
  at runtime via `/tmp/tps_control` without restarting the process.
- Injects a fixed set of scripted fraud scenarios at known trigger seconds — one per detection
  engine (Rules, CEP, ML) — plus a continuous stream of randomly weighted fraud patterns
  (`src/generator/fraud_simulator.py`), each labelled with `is_fraud` / `fraud_pattern` for later
  evaluation.
- Writes every generated transaction (label included) to `fraud_detection.ground_true_transactions`
  in ClickHouse (`src/utils/clickhouse_utils.py`), which the Flink pipeline's decisions can later
  be joined against to compute detection rate and false-positive rate.

## Directory Layout

```
src/
  main.py                     entry point: seeds default rules, then generates traffic
  producer.py                 Kafka producer helpers (transactions, rule updates)
  config/settings.py          TPS, time windows, fraud scenarios, rule updates, channels/locations
  generator/
    models.py                 Transaction / AccountProfile / FraudPattern definitions
    generate_accounts.py      one-off script: builds the synthetic account pool CSV
    transaction_generator.py  per-second batch generation, window-cycle bookkeeping
    fraud_simulator.py        one injector function per fraud pattern
  utils/
    clickhouse_utils.py       account loading, ground-truth writes, table truncation
    latency.py                end-to-end and rule-update latency queries
    fraud_eval.py             detection rate / false-positive rate from ground truth
    csv_utils.py              CSV writer helper
```

## Setup

```bash
pip install -r requirements.txt
```

Requires `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:29092`) and `CLICKHOUSE_HOST` (default
`localhost`) to point at a running stack — see `infra/reset.sh` / `docs/runbooks/local-setup.md`.

## Usage

1. **Seed the account pool** (one-time, or whenever `NUM_ACCOUNTS` changes in `config/settings.py`):
   ```bash
   python -m src.generator.generate_accounts
   clickhouse-client --query "INSERT INTO fraud_detection.accounts FORMAT CSVWithNames" < data/accounts.csv
   ```
2. **Run the generator**:
   ```bash
   python -m src.main --tps 5000
   ```
   Change throughput live without restarting: `echo 2000 > /tmp/tps_control`.
3. **Evaluate results** once the Flink pipeline has processed the stream:
   ```bash
   python -m src.utils.latency     
   python -m src.utils.fraud_eval  
   ```
4. **Reset state** between runs: `python -m src.utils.clickhouse_utils` truncates
   `fraud_detection.transactions` and `fraud_detection.ground_true_transactions`.

## Docker

The Dockerfile runs `python -m src.main` directly. In `infra/docker-compose.yml` this service is
tagged `profiles: ["seed"]`, so it is not started by a plain `docker compose up` — run it
explicitly with `docker compose --profile seed up transaction-generator`.
