import random
from datetime import timedelta

import numpy as np

from .config import RULE_CAP, CEP_LARGE_AMOUNT, START, END
from .models import Account, Row
from .samplers import _round_vnd


def inject_amount_anomaly(acc: Account, base_ts) -> list[Row]:
    amount = _round_vnd(min(acc.avg_amount * random.uniform(50, 500), RULE_CAP))
    return [Row(acc.account_id, amount, base_ts, True, True, True)]

def inject_weak_velocity(acc: Account, base_ts) -> list[Row]:
    base_amount = random.uniform(
        max(CEP_LARGE_AMOUNT * 1.2, acc.avg_amount * 30),
        min(acc.avg_amount * 200, RULE_CAP),
    )
    rows = []
    for offset_s in (0, 90, 130):
        ts = base_ts + timedelta(seconds=offset_s)
        if ts >= END:
            break
        amount = _round_vnd(base_amount * random.uniform(0.85, 1.15))
        rows.append(Row(acc.account_id, amount, ts, True, True, True))
    return rows

def inject_location_anomaly(acc: Account, base_ts) -> list[Row]:
    amount = _round_vnd(
        max(50_000.0, np.random.normal(acc.avg_amount * 2.5, acc.avg_amount * 0.5))
    )
    return [Row(acc.account_id, amount, base_ts, False, False, True)]

def inject_unusual_hour(acc: Account) -> list[Row]:
    ts     = START + timedelta(hours=random.uniform(14.0, 16.0))
    amount = _round_vnd(min(acc.avg_amount * random.uniform(3, 20), RULE_CAP))
    return [Row(acc.account_id, amount, ts, True, True, True)]

def inject_combined_weak(acc: Account) -> list[Row]:
    ts     = START + timedelta(hours=random.uniform(13.0, 15.0))
    amount = _round_vnd(acc.avg_amount * random.uniform(5, 10))
    return [Row(acc.account_id, amount, ts, False, True, True)]
