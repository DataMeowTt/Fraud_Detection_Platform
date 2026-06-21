import math
import random
from datetime import datetime, timedelta

import numpy as np

from .config import (
    TX_MIN, TX_MAX, TX_AVG, TX_STD,
    START, END, HOUR_WEIGHTS,
)
from .models import Account, Row


def _round_vnd(amount: float) -> float:
    return max(50_000.0, round(amount, -3))

def sample_avg_amount() -> float:
    return float(np.random.lognormal(math.log(2_000_000), 0.65))

def sample_n_tx() -> int:
    n = int(round(np.random.normal(TX_AVG, TX_STD)))
    return max(TX_MIN, min(TX_MAX, n))

def sample_timestamps(n: int) -> list[datetime]:
    hours = np.random.choice(np.arange(24), size=n, p=HOUR_WEIGHTS)
    secs  = np.random.uniform(0, 3600, size=n)
    raw   = sorted(
        START + timedelta(hours=int((h - 12) % 24), seconds=float(s))
        for h, s in zip(hours, secs)
    )
    result = [raw[0]]
    for ts in raw[1:]:
        if (ts - result[-1]).total_seconds() < 120:
            ts = result[-1] + timedelta(seconds=120)
        result.append(ts)
    return [min(ts, END - timedelta(seconds=1)) for ts in result]

def normal_row(acc: Account, ts: datetime) -> Row:
    amount = _round_vnd(np.random.normal(acc.avg_amount, acc.avg_amount * 0.15))
    return Row(acc.account_id, amount, ts, True, True, False)
