import argparse
import queue
import random
import threading
import time
from dataclasses import asdict
from datetime import datetime, timezone

from src.config.settings import (
    DOMESTIC_LOCS, KAFKA_BOOTSTRAP_SERVERS,
    TPS, START_DATE, WINDOWS, SCENARIOS, RULE_UPDATES, DEFAULT_RULES,
    FRAUD_RATIO,
)
from src.generator.fraud_simulator import INJECTOR_MAP
from src.generator.models import AccountProfile
from src.generator.transaction_generator import generate
from src.producer import create_producer, produce_batch, produce_rule_update
from src.utils.clickhouse_utils import get_client, read_accounts, insert_ground_truth_batch


def tps_file_watcher(tps_ref: list, stop_event: threading.Event, path: str = "/tmp/tps_control"):
    last_val = tps_ref[0]
    while not stop_event.is_set():
        try:
            with open(path) as f:
                val = int(f.read().strip())
            if val > 0 and val != last_val:
                tps_ref[0] = val
                last_val = val
                print(f"[TPS] → {val}", flush=True)
        except (FileNotFoundError, ValueError):
            pass
        time.sleep(1)


def flush_worker(ch_client, write_queue: queue.Queue, stop_event: threading.Event):
    while not stop_event.is_set() or not write_queue.empty():
        try:
            batch = write_queue.get(timeout=0.5)
            insert_ground_truth_batch(ch_client, batch)
            write_queue.task_done()
        except queue.Empty:
            continue


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tps", type=int, default=TPS)
    args = parser.parse_args()

    tps_ref = [args.tps]
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
    producer.flush()
    time.sleep(2)

    write_queue = queue.Queue()
    stop_event  = threading.Event()
    flush_thread = threading.Thread(
        target=flush_worker,
        args=(ch_client, write_queue, stop_event),
        daemon=True,
    )
    flush_thread.start()
    tps_watch_thread = threading.Thread(
        target=tps_file_watcher,
        args=(tps_ref, stop_event),
        daemon=True,
    )
    tps_watch_thread.start()
    print(f"[TPS] Starting at {tps_ref[0]} TPS  (change: docker exec <container> sh -c \"echo <val> > /tmp/tps_control\")")

    total = fraud_count = 0

    try:
        for sec, (batch, ws, we) in enumerate(generate(tps_ref, START_DATE, WINDOWS, accounts, fraud_scenarios,
                                                       random_injectors=INJECTOR_MAP,
                                                       fraud_ratio=FRAUD_RATIO)):
            deadline = time.monotonic() + 1.0

            if sec in RULE_UPDATES:
                produce_rule_update(producer, RULE_UPDATES[sec])
                rule_produced_at = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
                with open("rule_update_time.txt", "w") as f:
                    f.write(rule_produced_at)

            rows = [asdict(tx) for tx in batch]
            produce_batch(producer, rows)
            for tx, row in zip(batch, rows):
                tx.produced_at = row["produced_at"]
            write_queue.put(batch)

            fraud_count += sum(1 for tx in batch if tx.is_fraud)
            total       += len(batch)
            window_label = f"{ws.strftime('%H:%M')}-{we.strftime('%H:%M')}"
            print(f"[sec {sec:>4}] tps={tps_ref[0]:>5}  window={window_label}  sent {len(batch):>5} txs  fraud={fraud_count}")

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
