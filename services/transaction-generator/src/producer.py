import json
from datetime import datetime, timezone

from confluent_kafka import Producer

from src.utils.schema_validator import validate_transaction


def create_producer(bootstrap_servers: str) -> Producer:
    return Producer({"bootstrap.servers": bootstrap_servers, "acks": 1})


def produce_batch(producer: Producer, rows: list[dict]) -> None:
    for row in rows:
        row["produced_at"] = datetime.now(timezone.utc).isoformat()
        payload = {k: v for k, v in row.items() if k not in ["is_fraud", "fraud_pattern"]}
        validate_transaction(payload)
        producer.produce(
            "transactions",
            key=row["account_id"].encode(),
            value=json.dumps(payload).encode(),
        )
    producer.poll(0)


def produce_rule_update(producer: Producer, rule: dict) -> None:
    producer.produce(
        "rules-updates",
        key=rule["rule_id"].encode(),
        value=json.dumps(rule).encode(),
    )
    producer.poll(0)
