from dataclasses import dataclass
from datetime import datetime


@dataclass
class Account:
    account_id:    str
    avg_amount:    float
    home_location: str
    fraud_type:    str | None = None


@dataclass
class Row:
    account_id:  str
    amount:      float
    event_time:  datetime
    is_home:     bool
    is_domestic: bool
    is_fraud:    bool
