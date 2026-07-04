import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = "transactions"
ACCOUNT_ID_PREFIX = "LOAD_TEST_ACC"
CARD_ID_PREFIX    = "LOAD_TEST_CARD"
NUM_ACCOUNTS      = 5000
MIN_AMOUNT = 200_000
MAX_AMOUNT = 2_000_000
LEVELS_OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "load_test_levels.json")

def build_account_pool(num_accounts: int) -> list[dict]:
    return [
        {
            "account_id":    f"{ACCOUNT_ID_PREFIX}_{i:05d}",
            "card_id":       f"{CARD_ID_PREFIX}_{i:05d}",
            "home_location": "Hanoi",
        }
        for i in range(num_accounts)
    ]

def build_transaction(account: dict) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "transaction_id": f"LOAD_TEST_TX_{uuid.uuid4()}",
        "event_time":     now.isoformat(),
        "account_id":     account["account_id"],
        "card_id":        account["card_id"],
        "amount":         random.randint(MIN_AMOUNT, MAX_AMOUNT),
        "channel":        "ONLINE",
        "location_name":  "Hanoi",
        "status":         "Approved",
    }

def create_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "acks": 1})

def produce_batch(producer: Producer, accounts: list[dict], count: int, topic: str) -> None:
    for _ in range(count):
        row = build_transaction(random.choice(accounts))
        row["produced_at"] = datetime.now(timezone.utc).isoformat()
        producer.produce(topic, key=row["account_id"].encode(), value=json.dumps(row).encode())
    producer.poll(0)

def run_level(producer: Producer, accounts: list[dict], tps: int, duration_seconds: int, topic: str) -> dict:
    print(f"\n[LoadTest] level={tps} tps  duration={duration_seconds}s")
    level_start = datetime.now(timezone.utc)
    total_sent = 0

    for sec in range(1, duration_seconds + 1):
        deadline = time.monotonic() + 1.0
        produce_batch(producer, accounts, tps, topic)
        total_sent += tps
        print(f"  [sec {sec:>4}/{duration_seconds}] sent {tps:>5} txs  (total={total_sent})")
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)

    producer.flush()
    level_end = datetime.now(timezone.utc)
    return {"tps": tps, "start": level_start.isoformat(), "end": level_end.isoformat(), "sent": total_sent}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--levels", default="500,1000,3000,5000", help="comma-separated events/s levels")
    parser.add_argument("--duration", type=int, default=30, help="seconds to hold each level")
    parser.add_argument("--accounts", type=int, default=NUM_ACCOUNTS, help="size of synthetic account pool")
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--output", default=LEVELS_OUTPUT_PATH,
                         help="where to record level time windows for metrics_collector.py")
    args = parser.parse_args()

    levels = [int(x) for x in args.levels.split(",")]
    accounts = build_account_pool(args.accounts)
    producer = create_producer()

    results = []
    try:
        for tps in levels:
            results.append(run_level(producer, accounts, tps, args.duration, args.topic))
    except KeyboardInterrupt:
        print("\n[Interrupted]")
    finally:
        producer.flush()

    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n[LoadTest] Done. Level windows written to --> {args.output}")

if __name__ == "__main__":
    main()
