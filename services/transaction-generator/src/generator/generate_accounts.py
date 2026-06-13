import random

from src.config.settings import DOMESTIC_LOCS, NUM_ACCOUNTS
from src.generator.models import AccountProfile

OUTPUT = "/Users/trananhtuan/Documents/Fraud_Detection_Platform/data/accounts.csv"

AMOUNT_RANGES = [
    (0.70,  300_000,    2_000_000),
    (0.25, 2_000_000,  10_000_000),
    (0.05, 10_000_000, 50_000_000),
]

ACCOUNT_SCHEMA = ["account_id", "card_id", "home_location", "avg_amount"]


def build_account_pool() -> dict:
    
    amount_buckets = []
    for ratio, lo, hi in AMOUNT_RANGES:
        amount_buckets.extend([(lo, hi)] * int(NUM_ACCOUNTS * ratio))
        
    while len(amount_buckets) < NUM_ACCOUNTS:
        amount_buckets.append((300_000, 2_000_000))
    random.shuffle(amount_buckets)

    accounts = {}
    for i in range(NUM_ACCOUNTS):
        acc_id = f"ACC_{i:05d}"
        lo, hi = amount_buckets[i]
        accounts[acc_id] = AccountProfile(
            account_id    = acc_id,
            card_id       = f"CARD_{i:05d}",
            home_location = random.choice(DOMESTIC_LOCS),
            avg_amount    = random.uniform(lo, hi),
        )
        
    return accounts


if __name__ == "__main__":
    from src.utils.csv_utils import write_csv

    random.seed(42)
    accounts = build_account_pool()

    rows = [
        {
            "account_id":    acc.account_id,
            "card_id":      acc.card_id,
            "home_location": acc.home_location,
            "avg_amount":    f"{acc.avg_amount:.0f}",
        }
        for acc in accounts.values()
    ]
    write_csv(rows, OUTPUT, ACCOUNT_SCHEMA)
