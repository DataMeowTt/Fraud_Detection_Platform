from datetime import datetime, timedelta, timezone

import numpy as np

RANDOM_SEED  = 42
NUM_ACCOUNTS = 20_000

TX_MIN = 8
TX_MAX = 20
TX_AVG = 11.0
TX_STD = 3.0

FRAUD_COUNT = {
    "AMOUNT_ANOMALY":   1_000,
    "WEAK_VELOCITY":      700,
    "LOCATION_ANOMALY": 1_000,
    "UNUSUAL_HOUR":     1_000,
    "COMBINED_WEAK":      900,
}

DOMESTIC_LOCS = ["Hanoi", "HCM City", "Da Nang", "Can Tho", "Hai Phong"]
FOREIGN_LOCS  = ["Phnom Penh", "Bangkok", "Singapore", "Tokyo", "London", "New York"]

START = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
END   = START + timedelta(hours=24)

RULE_CAP         = 999_000_000
CEP_LARGE_AMOUNT =  50_000_000

_W = np.array([
    0.30, 0.25, 0.20, 0.20, 0.25, 0.35,
    1.20, 1.50, 1.50, 1.50, 1.50, 1.80,
    3.00, 3.00, 3.00, 3.00, 2.80, 2.50,
    2.00, 1.80, 1.50, 1.00, 0.60, 0.40,
])
HOUR_WEIGHTS = _W / _W.sum()
