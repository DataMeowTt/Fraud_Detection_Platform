import random
import uuid
from datetime import datetime, timedelta

from src.config.settings import CHANNELS, CHANNEL_WEIGHTS
from src.generator.models import AccountProfile, FraudPattern, Transaction

CYCLE_SECONDS = 24 * 3600
NIGHT_WIN_IDX = 9

RANDOM_PATTERNS = [
    "HIGH_AMOUNT_BLOCK",
    "HIGH_FREQUENCY",
    "LOCATION_JUMP",
    "DECLINED_BURST",
    "RAPID_MICROPAYMENTS",
    "UNDER_THREAD_ACTIVITY",
    "ADVANCED_LOCATION_JUMP",
    "ADVANCED_HIGH_FREQUENCY_V1",
    "ADVANCED_HIGH_FREQUENCY_V2",
]

class WindowContext:

    def __init__(self, windows: list, idx: int, cycle: int, start_date: datetime):
        self.windows    = windows
        self.idx        = idx
        self.cycle      = cycle
        self.start_date = start_date

    def compute(self, i: int, c: int) -> tuple[datetime, datetime]:
        offset_s, dur_s = self.windows[i]
        ws = self.start_date + timedelta(seconds=c * CYCLE_SECONDS + offset_s)
        
        return ws, ws + timedelta(seconds=dur_s)

    def next_window(self) -> tuple[datetime, datetime]:
        next_idx   = (self.idx + 1) % len(self.windows)
        next_cycle = self.cycle + 1 if next_idx == 0 else self.cycle
        
        return self.compute(next_idx, next_cycle)

    def night_window(self) -> tuple[datetime, datetime]:
        night_idx   = NIGHT_WIN_IDX % len(self.windows)
        current_off = self.windows[self.idx][0]
        night_off   = self.windows[night_idx][0]
        night_cycle = self.cycle if current_off < night_off else self.cycle + 1
        
        return self.compute(night_idx, night_cycle)

    def find_sec(self, dt: datetime) -> int:
        total_s = (dt - self.start_date).total_seconds()
        
        if total_s < 0:
            return -1
        
        cycle  = int(total_s // CYCLE_SECONDS)
        offset = total_s - cycle * CYCLE_SECONDS
        
        for i, (off_s, dur_s) in enumerate(self.windows):
            if off_s <= offset < off_s + dur_s:
                return cycle * len(self.windows) + i
            
        return -1

def make_transaction_amount_normal(account: AccountProfile) -> float:
    mu = account.avg_amount
    
    return max(50_000, round(random.gauss(mu, mu * 0.15), -3))

def make_transaction(account: AccountProfile,
                     event_dt: datetime,
                     amount: float,
                     location_name: str,
                     channel: str = "Online",
                     is_fraud: bool = False,
                     pattern: FraudPattern = FraudPattern.NONE,
                     status: str = "Approved") -> Transaction:
    ts_iso = event_dt.isoformat()
    
    return Transaction(
        transaction_id = str(uuid.uuid4()),
        produced_at    = ts_iso,
        event_time     = ts_iso,
        account_id     = account.account_id,
        card_id        = account.card_id,
        amount         = amount,
        channel        = channel,
        location_name  = location_name,
        status         = status,
        is_fraud       = is_fraud,
        fraud_pattern  = pattern.value,
    )

def make_normal_transaction(account: AccountProfile, event_dt: datetime) -> Transaction:
    
    return make_transaction(
        account, event_dt,
        amount        = make_transaction_amount_normal(account),
        location_name = account.home_location,
        channel       = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0],
        is_fraud      = False,
        pattern       = FraudPattern.NONE,
    )


def generate(tps: int,
             start_date: datetime,
             windows: list,
             accounts: dict,
             fraud_scenarios: list,
             random_injectors: dict = {},
             random_fraud_prob: float = 0.0):

    fraud_by_sec: dict[int, list] = {}
    for acc, injector, trigger_sec in fraud_scenarios:
        fraud_by_sec.setdefault(trigger_sec, []).append((acc, injector))

    acc_list = list(accounts.values())

    seen_stack: list[AccountProfile] = []
    seen_set:   set[str]             = set()
    pending:    dict[int, list]      = {}  # sec → txs được defer sang window sau

    sec = 0
    while True:
        cycle     = sec // len(windows)
        idx       = sec % len(windows)
        offset_s, duration_s = windows[idx]

        window_start = start_date + timedelta(seconds=cycle * CYCLE_SECONDS + offset_s)
        window_end   = window_start + timedelta(seconds=duration_s)
        ctx          = WindowContext(windows, idx, cycle, start_date)

        # Drain txs đã được defer cho sec này
        batch: list = pending.pop(sec, [])

        for _ in range(tps):
            acc = random.choice(acc_list)
            if acc.account_id not in seen_set:
                seen_set.add(acc.account_id)
                seen_stack.append(acc)
            batch.append(make_normal_transaction(
                acc,
                window_start + timedelta(seconds=random.uniform(0, duration_s)),
            ))

        if sec in fraud_by_sec:
            for acc, injector in fraud_by_sec[sec]:
                batch.extend(injector(acc, window_start, window_end))

        if random.random() < random_fraud_prob:
            pattern_name = random.choice(RANDOM_PATTERNS)
            rnd_acc      = random.choice(seen_stack)
            new_txs      = random_injectors[pattern_name](rnd_acc, window_start, window_end, ctx=ctx)

            for tx in new_txs:
                tx_dt = datetime.fromisoformat(tx.event_time)
                if window_start <= tx_dt < window_end:
                    batch.append(tx)
                else:
                    target = ctx.find_sec(tx_dt)
                    if target > sec:
                        pending.setdefault(target, []).append(tx)

        batch.sort(key=lambda tx: tx.event_time)
        yield batch, window_start, window_end
        sec += 1
