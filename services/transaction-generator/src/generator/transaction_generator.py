import csv
import random
import uuid
from dataclasses import asdict
from datetime import datetime, timezone

from src.config.settings import CHANNELS, CHANNEL_WEIGHTS
from src.generator.models import AccountProfile, FraudPattern, Transaction

EVENT_SCHEMA = [
    "transaction_id", 
    "produced_at", 
    "event_time",
    "account_id", 
    "card_id", 
    "amount",
    "channel", 
    "location_name",
    "status", 
    "is_fraud", 
    "fraud_pattern"
]

def make_transaction_amount_normal(account: AccountProfile) -> float:
    
    mu = account.avg_amount
    return max(50_000, round(random.gauss(mu, mu * 0.15), -3))

def make_transaction(account: AccountProfile, 
                     ts: float, 
                     amount: float, 
                     location_name: str, 
                     channel: str = "Online",
                     is_fraud: bool = False,
                     pattern: FraudPattern = FraudPattern.NONE,
                     status: str = "Approved") -> Transaction:
    
    event_ts = ts + random.uniform(-2, 2)
        
    return Transaction(
        transaction_id = str(uuid.uuid4()),
        produced_at    = datetime.fromtimestamp(ts,       tz=timezone.utc).isoformat(),
        event_time     = datetime.fromtimestamp(event_ts, tz=timezone.utc).isoformat(),
        account_id     = account.account_id,
        card_id        = account.card_id,
        amount         = amount,
        channel        = channel,
        location_name  = location_name,
        status         = status,
        is_fraud       = is_fraud,
        fraud_pattern  = pattern.value,
    )

def make_normal_transaction(account: AccountProfile, 
                            ts: float) -> Transaction:
    
    return make_transaction(
        account, ts,
        amount        = make_transaction_amount_normal(account),
        location_name = account.home_location,
        channel       = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0],
        is_fraud      = False,
        pattern       = FraudPattern.NONE,
    )

def read_accounts_from_csv(path: str) -> dict:
    
    accounts = {}
    with open(path, newline="", encoding="utf-8") as f:
        
        for row in csv.DictReader(f):
            acc = AccountProfile(
                account_id    = row["account_id"],
                card_id       = row["card_id"],
                home_location = row["home_location"],
                avg_amount    = float(row["avg_amount"]),
            )
            accounts[acc.account_id] = acc
            
    return accounts

def generate(tps: int,
             duration_sec: int,
             start_epoch: float,
             accounts: dict,
             fraud_scenarios: list):

    fraud_by_sec: dict[int, list] = {}
    for acc, injector, seconds in fraud_scenarios:

        timestamps = [start_epoch + sec + random.random() for sec in seconds]
        fraud_txs  = injector(acc, timestamps)
        
        for sec, tx in zip(seconds, fraud_txs):
            fraud_by_sec.setdefault(sec, []).append(tx)

    acc_list = list(accounts.values())

    for sec in range(duration_sec):
        batch = [
            make_normal_transaction(random.choice(acc_list), start_epoch + sec + random.random())
            for _ in range(tps)
        ]
        if sec in fraud_by_sec:
            batch.extend(fraud_by_sec[sec])
        batch.sort(key=lambda tx: tx.produced_at)
        
        yield batch


if __name__ == "__main__":
    import os
    import time
    
    from src.config.settings import DOMESTIC_LOCS
    from src.generator.fraud_simulator import INJECTOR_MAP

    TPS          = 25
    DURATION     = 20
    ACCOUNTS_CSV = "/Users/trananhtuan/Documents/Fraud_Detection_Platform/data/accounts.csv"
    OUTPUT       = "/Users/trananhtuan/Documents/Fraud_Detection_Platform/data/transactions.csv"
    
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    
    # [account_id, card_id, pattern_name, seconds]
    SCENARIOS = [
        ["FRD_00001", "FRD_CARD_00001", "HIGH_AMOUNT_BLOCK",   [3]],
        ["FRD_00002", "FRD_CARD_00002", "HIGH_FREQUENCY",      [2, 5, 8, 11]],
        ["FRD_00003", "FRD_CARD_00003", "LOCATION_JUMP",       [6, 10]],
        ["FRD_00004", "FRD_CARD_00004", "DECLINED_BURST",      [4, 7, 12]],
        ["FRD_00005", "FRD_CARD_00005", "RAPID_MICROPAYMENTS", [9, 13, 17]]
    ]

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

    total = fraud_count = 0
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=EVENT_SCHEMA)
        writer.writeheader()
        for batch in generate(
            tps             = TPS,
            duration_sec    = DURATION,
            start_epoch     = time.time(),
            accounts        = accounts,
            fraud_scenarios = fraud_scenarios,
        ):
            for tx in batch:
                row = asdict(tx)
                row["amount"]   = f"{tx.amount:.0f}"
                row["is_fraud"] = str(tx.is_fraud).upper()
                writer.writerow(row)
            fraud_count += sum(1 for tx in batch if tx.is_fraud)
            total += len(batch)

    print(f"[Generator] Done  total={total:,}  fraud={fraud_count:,}  rate={fraud_count/total*100:.2f}%")
    print(f"[CSV] Saved {total:,} rows → {OUTPUT}")
