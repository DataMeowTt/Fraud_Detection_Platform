import os

NUM_ACCOUNTS = 20_000
FRAUD_RATE   = 0.025

CHANNELS        = ["ATM", "POS", "ONLINE"]
CHANNEL_WEIGHTS = [0.25,  0.05,   0.70]
DOMESTIC_LOCS = ["Hanoi", "HCM City", "Da Nang", "Can Tho", "Hai Phong"]
FOREIGN_LOCS  = ["Phnom Penh", "Bangkok", "Singapore", "Tokyo", "London", "New York"]

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
