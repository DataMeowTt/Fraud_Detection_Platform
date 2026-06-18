import os
from datetime import datetime

import clickhouse_connect

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")

GROUND_TRUTH_COLS = [
    "transaction_id", "produced_at", "event_time", "account_id", "card_id",
    "amount", "channel", "location_name", "status", "is_fraud", "fraud_pattern",
]


def get_client():
    return clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=8123)

def read_accounts(client) -> dict:
    from src.generator.models import AccountProfile
    result = client.query("SELECT account_id, card_id, home_location, avg_amount FROM fraud_detection.accounts")
    accounts = {}
    for row in result.result_rows:
        acc = AccountProfile(
            account_id    = row[0],
            card_id       = row[1],
            home_location = row[2],
            avg_amount    = float(row[3]),
        )
        accounts[acc.account_id] = acc
    return accounts

def insert_ground_truth_batch(client, batch) -> None:
    rows = [
        [
            tx.transaction_id,
            datetime.fromisoformat(tx.produced_at),
            datetime.fromisoformat(tx.event_time),
            tx.account_id,
            tx.card_id,
            int(tx.amount),
            tx.channel,
            tx.location_name,
            tx.status,
            tx.is_fraud,
            tx.fraud_pattern,
        ]
        for tx in batch
    ]
    client.insert("fraud_detection.ground_true_transactions", rows, column_names=GROUND_TRUTH_COLS)

def truncate_ground_truth(client) -> None:
    client.command("TRUNCATE TABLE fraud_detection.ground_true_transactions")

def truncate_transactions(client) -> None:
    client.command("TRUNCATE TABLE fraud_detection.transactions")
    
def test_connection(client) -> bool:
    result = client.query("SELECT name FROM system.tables WHERE database = 'fraud_detection' ORDER BY name")
    tables = [row[0] for row in result.result_rows]
    print(f"[ClickHouse] Connected to {CLICKHOUSE_HOST}:8123")
    print(f"[ClickHouse] Tables in fraud_detection ({len(tables)}):")
    for t in tables:
        print(f"             - {t}")
    return len(tables) > 0

def main():
    client = get_client()
    try:
        truncate_transactions(client)
        truncate_ground_truth(client)
    finally:
        client.close()


if __name__ == "__main__":
    main()
