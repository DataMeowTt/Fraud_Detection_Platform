import random

from src.config.settings import CHANNELS, FOREIGN_LOCS, CHANNEL_WEIGHTS
from src.generator.models import AccountProfile, FraudPattern
from src.generator.transaction_generator import make_transaction, make_transaction_amount_normal

def make_high_amount_transaction(account: AccountProfile, base_times: list[float]) -> list:
    amount = round(random.uniform(100, 200) * account.avg_amount, -3)
    return [make_transaction(account, base_times[0], amount,
                             account.home_location, random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0],
                             True, FraudPattern.HIGH_AMOUNT_BLOCK)]

def make_high_frequency(account: AccountProfile, base_times: list[float]) -> list:
    
    return [
        make_transaction(account, 
                         base_time,
                         round(random.uniform(20, 100) * account.avg_amount, -3),
                         account.home_location, 
                         random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0],
                         True, 
                         FraudPattern.HIGH_FREQUENCY)
        
        for base_time in base_times
    ]

def make_location_jump_transaction(account: AccountProfile, base_times: list[float]) -> list:

    foreign_location = random.choice(FOREIGN_LOCS)
    
    return [
        make_transaction(account, 
                         base_times[0], 
                         make_transaction_amount_normal(account),
                         account.home_location
        ),
        make_transaction(account, 
                         base_times[1],
                         round(random.uniform(20, 100) * account.avg_amount, -3),
                         foreign_location, 
                         random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0],
                         True, 
                         FraudPattern.LOCATION_JUMP),
    ]

def make_declined_burst_transaction(account: AccountProfile, base_times: list[float]) -> list:
    
    return [
        make_transaction(account, 
                         base_time,
                         round(random.uniform(20, 100) * account.avg_amount, -3),
                         account.home_location, 
                         random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0],
                         True, 
                         FraudPattern.DECLINED_BURST,
                         status = "Declined")
        
        for base_time in base_times
    ]

def make_rapid_micro_transaction(account: AccountProfile, base_times: list[float]) -> list:
    
    return [
        make_transaction(account, 
                         base_time,
                         round(random.uniform(0.1, 0.5) * account.avg_amount, -3),
                         account.home_location, 
                         random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0],
                         True, 
                         FraudPattern.RAPID_MICROPAYMENTS)
        
        for base_time in base_times
    ]


INJECTOR_MAP = {
    "HIGH_AMOUNT_BLOCK":   make_high_amount_transaction,
    "HIGH_FREQUENCY":      make_high_frequency,
    "LOCATION_JUMP":       make_location_jump_transaction,
    "DECLINED_BURST":      make_declined_burst_transaction,
    "RAPID_MICROPAYMENTS": make_rapid_micro_transaction,
}
