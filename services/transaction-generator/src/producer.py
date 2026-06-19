import json
import random
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

def create_producer(bootstrap_servers: str) -> Producer:
    return Producer({"bootstrap.servers": bootstrap_servers, "acks": 1})

def produce_batch(producer: Producer, rows: list[dict]) -> None:
    for row in rows:
        now = datetime.now(timezone.utc)
        row["produced_at"] = now.isoformat()
        row["event_time"]  = (now - timedelta(seconds=random.uniform(0.001, 0.010))).isoformat()
        payload = {k: v for k, v in row.items() if k not in ["is_fraud", "fraud_pattern"]}
        producer.produce(
            "transactions",
            key=row["account_id"].encode(),
            value=json.dumps(payload).encode(),
        )
    producer.poll(0)
