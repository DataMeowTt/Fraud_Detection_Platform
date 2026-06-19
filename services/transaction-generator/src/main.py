import random
import time
from dataclasses import asdict

from src.config.settings import DOMESTIC_LOCS, KAFKA_BOOTSTRAP_SERVERS
from src.generator.fraud_simulator import INJECTOR_MAP
from src.generator.models import AccountProfile
from src.generator.transaction_generator import generate
from src.producer import create_producer, produce_batch
from src.utils.clickhouse_utils import get_client, read_accounts, insert_ground_truth_batch

TPS      = 25
DURATION = 60

SCENARIOS = [
    ["FRD_00001", "FRD_CARD_00001", "HIGH_AMOUNT_BLOCK",   [3]],
    ["FRD_00002", "FRD_CARD_00002", "HIGH_FREQUENCY",      [2, 5, 8]],
    ["FRD_00003", "FRD_CARD_00003", "LOCATION_JUMP",       [6, 10]],
    ["FRD_00004", "FRD_CARD_00004", "DECLINED_BURST",      [4, 7, 12]],
    ["FRD_00005", "FRD_CARD_00005", "RAPID_MICROPAYMENTS", [9, 13, 17]],
]


def main():
    random.seed(42)

    ch_client = get_client()
    accounts  = read_accounts(ch_client)
    print(f"[ClickHouse] Loaded {len(accounts):,} accounts")

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

    total = fraud_count = 0

    for batch in generate(TPS, DURATION, time.time(), accounts, fraud_scenarios):
        deadline = time.monotonic() + 1.0

        rows = [asdict(tx) for tx in batch]
        produce_batch(producer, rows)
        for tx, row in zip(batch, rows):
            tx.produced_at = row["produced_at"]
            tx.event_time  = row["event_time"]
        insert_ground_truth_batch(ch_client, batch)

        fraud_count += sum(1 for tx in batch if tx.is_fraud)
        total       += len(batch)
        print(f"[sec {total // TPS:>4}] sent {len(batch):>4} txs  fraud={fraud_count}")

        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)

    producer.flush()
    ch_client.close()
    print(f"[Done] total={total:,}  fraud={fraud_count:,}  rate={fraud_count/total*100:.2f}%")


if __name__ == "__main__":
    main()
