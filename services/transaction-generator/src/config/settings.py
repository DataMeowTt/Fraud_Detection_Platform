import os
from datetime import datetime, timezone

NUM_ACCOUNTS = 100_000

CHANNELS        = ["ATM", "POS", "ONLINE"]
CHANNEL_WEIGHTS = [0.25,  0.05,   0.70]
DOMESTIC_LOCS = ["Hanoi", "HCM City", "Da Nang", "Can Tho", "Hai Phong"]
FOREIGN_LOCS  = ["Phnom Penh", "Bangkok", "Singapore", "Tokyo", "London", "New York"]

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

# Transaction generator 

TPS               = 5000
FRAUD_RATIO = (0.020, 0.025)
START_DATE = datetime(2026, 6, 24, 16, 0, 0, tzinfo=timezone.utc)

WINDOWS = [
    ( 0 * 3600,  3600),  # 16:00-17:00
    ( 1 * 3600,  3600),  # 17:00-18:00
    ( 2 * 3600,  3600),  # 18:00-19:00
    ( 3 * 3600,  3600),  # 19:00-20:00
    ( 4 * 3600,  3600),  # 20:00-21:00
    ( 5 * 3600,  3600),  # 21:00-22:00
    ( 6 * 3600,  3600),  # 22:00-23:00
    ( 7 * 3600,  3600),  # 23:00-00:00
    ( 8 * 3600,  3600),  # 00:00-01:00
    ( 9 * 3600, 18000),  # 01:00-06:00  (5h)
    (14 * 3600,  3600),  # 06:00-07:00
    (15 * 3600,  3600),  # 07:00-08:00
    (16 * 3600,  3600),  # 08:00-09:00
    (17 * 3600,  3600),  # 09:00-10:00
    (18 * 3600,  3600),  # 10:00-11:00
    (19 * 3600,  3600),  # 11:00-12:00
    (20 * 3600,  3600),  # 12:00-13:00
    (21 * 3600,  3600),  # 13:00-14:00
    (22 * 3600,  3600),  # 14:00-15:00
    (23 * 3600,  3600),  # 15:00-16:00
]

# [account_id, card_id, pattern, trigger_sec] 
SCENARIOS = [
    ["FRD_RULE_SC1",       "FRD_CARD_00001", "HIGH_AMOUNT_BLOCK",    3],
    ["FRD_CEP_SC2",        "FRD_CARD_00002", "HIGH_FREQUENCY",       2],
    ["FRD_CEP_SC3",        "FRD_CARD_00003", "LOCATION_JUMP",        5],
    ["FRD_CEP_SC4",        "FRD_CARD_00004", "DECLINED_BURST",       4],
    ["FRD_CEP_SC5",        "FRD_CARD_00005", "RAPID_MICROPAYMENTS",  6],
    
    # TESTING SCENARIOS FOR RULE UPDATE
    ["FRD_RULE_UPDATE_SC6","FRD_CARD_00006", "BLACKLISTED_ACCOUNT", 8],
    
    # ML SCENARIOS
    ["FRD_ML_SC7",         "FRD_CARD_00007", "WARM_UP_ACTIVITY",     3],
    ["FRD_ML_SC7",         "FRD_CARD_00007", "MIDNIGHT_ACTIVITY",   10],
]

RULE_UPDATES = {
    10: {"rule_id": "BLACKLIST_FRD_SC6", "field": "account_id", "operator": "EQ",
         "value": "FRD_RULE_UPDATE_SC6", "threshold": 0, "action": "BLOCK", "enabled": True},
}

DEFAULT_RULES = [
    {"rule_id": "AMOUNT_THRESHOLD", "field": "amount", "operator": "GT",
     "threshold": 1_000_000_000, "action": "BLOCK", "enabled": True},
]
