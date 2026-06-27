import queue
import random
import threading
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from src.config.settings import (
    DOMESTIC_LOCS, KAFKA_BOOTSTRAP_SERVERS,
    TPS, START_DATE, WINDOWS, SCENARIOS, RULE_UPDATES, DEFAULT_RULES,
    RANDOM_FRAUD_PROB,
)
from src.generator.fraud_simulator import INJECTOR_MAP
from src.generator.models import AccountProfile
from src.generator.transaction_generator import generate, make_normal_transaction
from src.producer import create_producer, produce_batch, produce_rule_update
from src.utils.clickhouse_utils import get_client, read_accounts, insert_ground_truth_batch


def flush_worker(ch_client, write_queue: queue.Queue, stop_event: threading.Event):
    while not stop_event.is_set() or not write_queue.empty():
        try:
            batch = write_queue.get(timeout=0.5)
            insert_ground_truth_batch(ch_client, batch)
            write_queue.task_done()
        except queue.Empty:
            continue


def main():
    random.seed(42)

    ch_client = get_client()
    accounts  = read_accounts(ch_client)
    print(f"[ClickHouse] Loaded {len(accounts):,} accounts")

    fraud_scenarios = []
    for acc_id, card_id, pattern_name, trigger_sec in SCENARIOS:
        acc = AccountProfile(
            account_id    = acc_id,
            card_id       = card_id,
            home_location = random.choice(DOMESTIC_LOCS),
            avg_amount    = 2_000_000,
        )
        fraud_scenarios.append((acc, INJECTOR_MAP[pattern_name], trigger_sec))

    producer = create_producer(KAFKA_BOOTSTRAP_SERVERS)

    for rule in DEFAULT_RULES:
        produce_rule_update(producer, rule)
    print(f"[Rules] Seeded {len(DEFAULT_RULES)} default rule(s)")

    rule_update_acc = AccountProfile(
        account_id    = "FRD_RULE_UPDATE_SC6",
        card_id       = "FRD_CARD_00006",
        home_location = random.choice(DOMESTIC_LOCS),
        avg_amount    = 2_000_000,
    )

    write_queue = queue.Queue()
    stop_event  = threading.Event()
    flush_thread = threading.Thread(
        target=flush_worker,
        args=(ch_client, write_queue, stop_event),
        daemon=True,
    )
    flush_thread.start()

    total = fraud_count = 0

    try:
        for sec, (batch, ws, we) in enumerate(generate(TPS, START_DATE, WINDOWS, accounts, fraud_scenarios,
                                                       random_injectors=INJECTOR_MAP,
                                                       random_fraud_prob=RANDOM_FRAUD_PROB)):
            deadline = time.monotonic() + 1.0

            if sec in RULE_UPDATES:
                produce_rule_update(producer, RULE_UPDATES[sec])
                rule_produced_at = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
                with open("rule_update_time.txt", "w") as f:
                    f.write(rule_produced_at)

                burst = [
                    make_normal_transaction(
                        rule_update_acc,
                        ws + timedelta(seconds=random.uniform(0, (we - ws).total_seconds())),
                    )
                    for _ in range(500)
                ]
                burst_rows = [asdict(tx) for tx in burst]
                produce_batch(producer, burst_rows)
                producer.flush()
                for tx, row in zip(burst, burst_rows):
                    tx.produced_at = row["produced_at"]
                write_queue.put(burst)
                total += len(burst)
                print(f"[Rules] burst {len(burst)} txs for {rule_update_acc.account_id}")

                remaining = deadline - time.monotonic()
                if remaining > 0:
                    time.sleep(remaining)
                continue

            rows = [asdict(tx) for tx in batch]
            produce_batch(producer, rows)
            for tx, row in zip(batch, rows):
                tx.produced_at = row["produced_at"]
            write_queue.put(batch)

            fraud_count += sum(1 for tx in batch if tx.is_fraud)
            total       += len(batch)
            window_label = f"{ws.strftime('%H:%M')}-{we.strftime('%H:%M')}"
            print(f"[sec {sec:>4}] window={window_label}  sent {len(batch):>5} txs  fraud={fraud_count}")

            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)

    except KeyboardInterrupt:
        print("\n[Interrupted]")
    finally:
        stop_event.set()
        flush_thread.join()
        producer.flush()
        ch_client.close()
        if total > 0:
            print(f"[Done] total={total:,}  fraud={fraud_count:,}  rate={fraud_count/total*100:.2f}%")


if __name__ == "__main__":
    main()
