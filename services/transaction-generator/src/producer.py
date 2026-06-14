import json

from confluent_kafka import Producer

def create_producer(bootstrap_servers: str) -> Producer:
    return Producer({"bootstrap.servers": bootstrap_servers})

def produce_batch(producer: Producer, rows: list[dict]) -> None:
    for row in rows:
        payload = {k: v for k, v in row.items() if k not in ["is_fraud", "fraud_pattern"]}
        producer.produce(
            "transactions",
            key=row["account_id"].encode(),
            value=json.dumps(payload).encode(),
        )
    producer.poll(0)
