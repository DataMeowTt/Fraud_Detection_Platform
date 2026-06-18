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
    
    return Transaction(
        transaction_id = str(uuid.uuid4()),
        produced_at    = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        event_time     = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
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

        timestamps = [start_epoch + sec for sec in seconds]
        fraud_txs  = injector(acc, timestamps)
        
        for sec, tx in zip(seconds, fraud_txs):
            fraud_by_sec.setdefault(sec, []).append(tx)

    acc_list = list(accounts.values())

    for sec in range(duration_sec):
        batch = [
            make_normal_transaction(random.choice(acc_list), start_epoch + sec + i / tps)
            for i in range(tps)
        ]
        if sec in fraud_by_sec:
            batch.extend(fraud_by_sec[sec])
        batch.sort(key=lambda tx: tx.produced_at)
        
        yield batch
