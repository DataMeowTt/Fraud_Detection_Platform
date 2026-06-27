from dataclasses import dataclass
from enum import Enum

class FraudPattern(str, Enum):
    NONE                         = "NONE"
    HIGH_AMOUNT_BLOCK            = "HIGH_AMOUNT_BLOCK"             # >1000x avg/ Rules BLOCK
    HIGH_FREQUENCY               = "HIGH_FREQUENCY"                # nhiều tx liên tiếp, 5x - 10x avg
    LOCATION_JUMP                = "LOCATION_JUMP"                 # giao dịch từ 2 quốc gia 
    DECLINED_BURST               = "DECLINED_BURST"                # 3 status DECLINED liên tiếp cùng account
    RAPID_MICROPAYMENTS          = "RAPID_MICROPAYMENTS"           # nhiều tx nhỏ liên tiếp
    BLACKLISTED_ACCOUNT          = "BLACKLISTED_ACCOUNT"           # account bị blacklist bởi rule động
    MIDNIGHT_ACTIVITY            = "MIDNIGHT_ACTIVITY"             # giao dịch lớn lúc 2-4h sáng (ML)
    UNDER_THREAD_ACTIVITY        = "UNDER_THREAD_ACTIVITY"         # giao dịch lớn dưới ngưỡng rule 1B (ML)
    ADVANCED_LOCATION_JUMP       = "ADVANCED_LOCATION_JUMP"        # location jump né CEP 1h window (ML)
    ADVANCED_HIGH_FREQUENCY_V1   = "ADVANCED_HIGH_FREQUENCY_V1"    # nhiều tx 70-99M cùng 1 window (ML)
    ADVANCED_HIGH_FREQUENCY_V2   = "ADVANCED_HIGH_FREQUENCY_V2"    # nhiều tx trải 3 window, né CEP (ML)


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
