import csv
import os
import random
import time
from dataclasses import asdict

from src.config.settings import DOMESTIC_LOCS, KAFKA_BOOTSTRAP_SERVERS
from src.generator.fraud_simulator import INJECTOR_MAP
from src.generator.models import AccountProfile
from src.generator.transaction_generator import EVENT_SCHEMA, generate, read_accounts_from_csv
from src.producer import create_producer, produce_batch

TPS          = 25
DURATION     = 20
ACCOUNTS_CSV = "/Users/trananhtuan/Documents/Fraud_Detection_Platform/data/accounts.csv"
OUTPUT_CSV   = "/Users/trananhtuan/Documents/Fraud_Detection_Platform/data/transactions.csv"
TOPIC        = "transactions"

SCENARIOS = [
    ["FRD_00001", "FRD_CARD_00001", "HIGH_AMOUNT_BLOCK",   [3]],
    ["FRD_00002", "FRD_CARD_00002", "HIGH_FREQUENCY",      [2, 5, 8, 11]],
    ["FRD_00003", "FRD_CARD_00003", "LOCATION_JUMP",       [6, 10]],
    ["FRD_00004", "FRD_CARD_00004", "DECLINED_BURST",      [4, 7, 12]],
    ["FRD_00005", "FRD_CARD_00005", "RAPID_MICROPAYMENTS", [9, 13, 17]],
]

def to_row(tx) -> dict:
    row = asdict(tx)
    row["amount"]   = f"{tx.amount:.0f}"
    row["is_fraud"] = str(tx.is_fraud).upper()
    return row

def main():
    random.seed(42)
    accounts = read_accounts_from_csv(ACCOUNTS_CSV)

    fraud_scenarios = []
    for acc_id, card_id, pattern_name, seconds in SCENARIOS:
        acc = AccountProfile(
            account_id    = acc_id,
            card_id       = card_id,
            home_location = random.choice(DOMESTIC_LOCS),
            avg_amount    = 2_000_000,
        )
        fraud_scenarios.append((acc, INJECTOR_MAP[pattern_name], seconds))

    producer = create_producer(KAFKA_BOOTSTRAP_SERVERS)
    os.makedirs("data", exist_ok=True)

    total = fraud_count = 0

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=EVENT_SCHEMA)
        writer.writeheader()

        for batch in generate(TPS, DURATION, time.time(), accounts, fraud_scenarios):
            deadline = time.monotonic() + 1.0

            rows = [to_row(tx) for tx in batch]

            writer.writerows(rows)
            produce_batch(producer, rows)

            fraud_count += sum(1 for tx in batch if tx.is_fraud)
            total       += len(batch)
            print(f"[sec {total // TPS:>4}] sent {len(batch):>4} txs  fraud={fraud_count}")

            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)

    producer.flush()
    print(f"[Done] total={total:,}  fraud={fraud_count:,}  rate={fraud_count/total*100:.2f}%")
    print(f"[CSV] Saved → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
