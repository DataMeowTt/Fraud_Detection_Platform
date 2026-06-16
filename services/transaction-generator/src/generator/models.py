from dataclasses import dataclass
from enum import Enum


class FraudPattern(str, Enum):
    NONE                  = "NONE"
    HIGH_AMOUNT_BLOCK     = "HIGH_AMOUNT_BLOCK"      # >1000x avg/ Rules BLOCK
    HIGH_FREQUENCY        = "HIGH_FREQUENCY"         # nhiều tx liên tiếp, 5x - 10x avg
    LOCATION_JUMP         = "LOCATION_JUMP"          # giao dịch từ 2 quốc gia 
    DECLINED_BURST        = "DECLINED_BURST"         # 3 status DECLINED liên tiếp cùng account
    RAPID_MICROPAYMENTS   = "RAPID_MICROPAYMENTS"    # nhiều tx nhỏ liên tiếp


@dataclass
class Transaction:
    transaction_id: str
    produced_at:    str
    event_time:     str
    account_id:     str
    card_id:        str
    amount:         float
    channel:        str
    location_name:  str
    status:         str
    is_fraud:       bool
    fraud_pattern:  str


@dataclass
class AccountProfile:
    account_id:    str
    card_id:       str
    home_location: str
    avg_amount:    float
