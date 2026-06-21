import random
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from .config import RANDOM_SEED, NUM_ACCOUNTS, FRAUD_COUNT, DOMESTIC_LOCS, END
from .injectors import (
    inject_amount_anomaly,
    inject_combined_weak,
    inject_location_anomaly,
    inject_unusual_hour,
    inject_weak_velocity,
)
from .models import Account, Row
from .samplers import normal_row, sample_avg_amount, sample_n_tx, sample_timestamps


def generate() -> pd.DataFrame:
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    accounts: list[Account] = [
        Account(
            account_id    = f"ML_{i+1:05d}",
            avg_amount    = sample_avg_amount(),
            home_location = random.choice(DOMESTIC_LOCS),
        )
        for i in range(NUM_ACCOUNTS)
    ]

    shuffled = accounts[:]
    random.shuffle(shuffled)
    ptr = 0
    for fraud_type, count in FRAUD_COUNT.items():
        for acc in shuffled[ptr : ptr + count]:
            acc.fraud_type = fraud_type
        ptr += count

    all_rows: list[Row] = []

    for acc in accounts:
        n_tx       = sample_n_tx()
        timestamps = sample_timestamps(n_tx)

        for ts in timestamps:
            all_rows.append(normal_row(acc, ts))

        if acc.fraud_type is None:
            continue

        fraud_after_idx = random.randint(3, n_tx - 1)
        ref_ts          = timestamps[fraud_after_idx]

        if acc.fraud_type == "AMOUNT_ANOMALY":
            fraud_ts = ref_ts + timedelta(minutes=random.uniform(5, 30))
            if fraud_ts < END:
                all_rows.extend(inject_amount_anomaly(acc, fraud_ts))

        elif acc.fraud_type == "WEAK_VELOCITY":
            fraud_ts = min(
                ref_ts + timedelta(minutes=random.uniform(5, 30)),
                END - timedelta(seconds=131),
            )
            all_rows.extend(inject_weak_velocity(acc, fraud_ts))

        elif acc.fraud_type == "LOCATION_ANOMALY":
            fraud_ts = min(
                ref_ts + timedelta(hours=random.uniform(2.0, 5.0)),
                END - timedelta(minutes=5),
            )
            all_rows.extend(inject_location_anomaly(acc, fraud_ts))

        elif acc.fraud_type == "UNUSUAL_HOUR":
            all_rows.extend(inject_unusual_hour(acc))

        elif acc.fraud_type == "COMBINED_WEAK":
            all_rows.extend(inject_combined_weak(acc))

    df = pd.DataFrame([
        {
            "account_id":  r.account_id,
            "amount":      r.amount,
            "event_time":  r.event_time.isoformat(),
            "is_home":     r.is_home,
            "is_domestic": r.is_domestic,
            "is_fraud":    r.is_fraud,
        }
        for r in all_rows
    ])

    return df.sort_values("event_time").reset_index(drop=True)


if __name__ == "__main__":
    df = generate()

    out_path = Path(__file__).parent.parent / "data" / "data_training.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    total = len(df)
    fraud = int(df["is_fraud"].sum())
    print(f"Total rows : {total:,}")
    print(f"Fraud rows : {fraud:,}  ({fraud / total * 100:.2f} %)")
    print(f"Saved to   : {out_path}")
