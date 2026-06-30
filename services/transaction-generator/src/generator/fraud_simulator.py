import random
from datetime import datetime, timedelta

from src.config.settings import CHANNELS, CHANNEL_WEIGHTS, DOMESTIC_LOCS, FOREIGN_LOCS
from src.generator.models import AccountProfile, FraudPattern
from src.generator.transaction_generator import (
    WindowContext,
    make_transaction,
    make_transaction_amount_normal,
)


def rand_date(ws: datetime, we: datetime) -> datetime:
    span = (we - ws).total_seconds()
    return ws + timedelta(seconds=random.uniform(0, span))


def pick_location(home: str) -> str:
    roll = random.random()
    if roll < 0.5:
        others = [loc for loc in DOMESTIC_LOCS if loc != home]
        return random.choice(others or DOMESTIC_LOCS)
    elif roll < 0.8:
        return random.choice(FOREIGN_LOCS)
    else:
        return home


def make_high_amount_transaction(account: AccountProfile, ws: datetime, we: datetime, *, ctx=None) -> list:
    time   = rand_date(ws, we)
    amount = random.randint(1_001_000_000, 2_000_000_000)
    return [make_transaction(account, time, amount, account.home_location,
                             random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0],
                             True, FraudPattern.HIGH_AMOUNT_BLOCK)]


def make_high_frequency(account: AccountProfile, ws: datetime, we: datetime, *, ctx=None) -> list:
    max_base_offset = max(0.0, (we - ws).total_seconds() - 3600)
    base    = ws + timedelta(seconds=random.uniform(0, max_base_offset))
    sub_end = min(base + timedelta(seconds=3600), we)
    channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0]

    span = (sub_end - base).total_seconds()
    times = sorted(base + timedelta(seconds=random.uniform(0, span)) for _ in range(3))
    return [
        make_transaction(account, t,
                         random.randint(100_001_000, 150_000_000),
                         account.home_location,
                         channel,
                         i > 0, FraudPattern.HIGH_FREQUENCY)
        for i, t in enumerate(times)
    ]


def make_location_jump_transaction(account: AccountProfile, ws: datetime, we: datetime, *, ctx=None) -> list:
    max_base_offset = max(0.0, (we - ws).total_seconds() - 3600)
    base = ws + timedelta(seconds=random.uniform(0, max_base_offset))
    gap  = random.uniform(1, min(3600, (we - base).total_seconds()))
    t2   = base + timedelta(seconds=gap)

    return [
        make_transaction(account, base,
                         make_transaction_amount_normal(account),
                         account.home_location,
                         random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0],
                         False, FraudPattern.NONE),
        make_transaction(account, t2,
                         round(random.uniform(20, 100) * account.avg_amount, -3),
                         random.choice(FOREIGN_LOCS),
                         random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0],
                         True, FraudPattern.LOCATION_JUMP),
    ]


def make_declined_burst_transaction(account: AccountProfile, ws: datetime, we: datetime, *, ctx=None) -> list:
    max_base_offset = max(0.0, (we - ws).total_seconds() - 120)
    base = ws + timedelta(seconds=random.uniform(0, max_base_offset))
    channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0]

    times = sorted(base + timedelta(seconds=random.uniform(0, 120)) for _ in range(3))
    return [
        make_transaction(account, t,
                         round(random.uniform(3, 10) * account.avg_amount, -3),
                         account.home_location,
                         channel,
                         i == 2, FraudPattern.DECLINED_BURST,
                         status="Declined")
        for i, t in enumerate(times)
    ]


def make_rapid_micro_transaction(account: AccountProfile, ws: datetime, we: datetime, *, ctx=None) -> list:
    max_base_offset = max(0.0, (we - ws).total_seconds() - 120)
    base = ws + timedelta(seconds=random.uniform(0, max_base_offset))

    times = sorted(base + timedelta(seconds=random.uniform(0, 120)) for _ in range(3))
    return [
        make_transaction(account, t,
                         random.randint(2_000, 10_000),
                         account.home_location,
                         'ONLINE',
                         i == 2, FraudPattern.RAPID_MICROPAYMENTS)
        for i, t in enumerate(times)
    ]


def make_black_list_transaction(account: AccountProfile, ws: datetime, we: datetime, *, ctx=None) -> list:
    return [
        make_transaction(account, rand_date(ws, we),
                         make_transaction_amount_normal(account),
                         account.home_location,
                         random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0],
                         True, FraudPattern.BLACKLISTED_ACCOUNT)
    ]


def make_warm_up_activity(account: AccountProfile, ws: datetime, we: datetime, *, ctx=None) -> list:
    # 1 giao dịch bình thường để FeatureExtractor có avg_amount_24h và tx_count làm baseline
    return [
        make_transaction(account, rand_date(ws, we),
                         make_transaction_amount_normal(account),
                         account.home_location,
                         random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0],
                         False, FraudPattern.NONE)
    ]


def make_midnight_activity(account: AccountProfile, ws: datetime, we: datetime, *, ctx=None) -> list:
    sub_start = ws.replace(hour=2, minute=0, second=0, microsecond=0)
    sub_end   = ws.replace(hour=4, minute=0, second=0, microsecond=0)
    location = pick_location(account.home_location)

    return [
        make_transaction(account,
                         rand_date(sub_start, sub_end),
                         random.randint(700_001_000, 999_999_000),
                         location,
                         random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0],
                         True, FraudPattern.MIDNIGHT_ACTIVITY)
        for _ in range(2)
    ]


def make_under_thread_activity(account: AccountProfile, ws: datetime, we: datetime, *, ctx=None) -> list:
    n        = random.randint(1, 2)
    location = pick_location(account.home_location)
    return [
        make_transaction(account,
                         rand_date(ws, we),
                         random.randint(600_001_000, 999_999_999),
                         location,
                         random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0],
                         True, FraudPattern.UNDER_THREAD_ACTIVITY)
        for _ in range(n)
    ]


def make_advanced_location_jump(account: AccountProfile, ws: datetime, we: datetime, *, ctx: WindowContext = None) -> list:
    if ctx is None:
        raise ValueError("make_advanced_location_jump requires ctx")

    # Chọn W2: 50% window liên tiếp, 50% window đêm 01:00-06:00
    w2_s, w2_e = ctx.next_window() if random.random() < 0.5 else ctx.night_window()

    # Chọn fraud location: 50% foreign, 50% domestic khác home
    if random.random() < 0.5:
        fraud_loc = random.choice(FOREIGN_LOCS)
    else:
        others    = [loc for loc in DOMESTIC_LOCS if loc != account.home_location]
        fraud_loc = random.choice(others or DOMESTIC_LOCS)

    # 1-2 normal tx trong W1
    n_normal     = random.randint(1, 2)
    normal_times = sorted([rand_date(ws, we) for _ in range(n_normal)])
    normal_txs   = [
        make_transaction(account, t,
                         random.randint(500_001, 2_000_000),
                         account.home_location,
                         random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0],
                         False, FraudPattern.NONE)
        for t in normal_times
    ]
    t_last_normal = normal_times[-1]

    # Fraud tx đầu tiên phải cách normal cuối > 1h (né CEP LocationJump 1h window)
    fraud_start = max(t_last_normal + timedelta(seconds=3_601), w2_s)
    if fraud_start >= w2_e:
        return normal_txs  # W2 quá hẹp, bỏ qua fraud

    available_s = (w2_e - fraud_start).total_seconds()
    n_fraud     = random.choice([3, 5])
    fraud_times = sorted([
        fraud_start + timedelta(seconds=random.uniform(0, available_s))
        for _ in range(n_fraud)
    ])

    # 2 tx đầu: 100-300M; từ tx thứ 3: 70-99M (né CEP HighFrequency 1h window)
    fraud_txs = [
        make_transaction(account, t,
                         random.randint(250_000_000, 500_000_000) if i < 2
                         else random.randint(70_000_000, 99_999_999),
                         fraud_loc,
                         random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0],
                         True, FraudPattern.ADVANCED_LOCATION_JUMP)
        for i, t in enumerate(fraud_times)
    ]

    return normal_txs + fraud_txs


def make_advanced_high_frequency_v1(account: AccountProfile, ws: datetime, we: datetime, *, ctx=None) -> list:
    n_tx    = random.randint(3, 6)
    channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0]
    span    = (we - ws).total_seconds()
    times   = sorted(ws + timedelta(seconds=random.uniform(0, span)) for _ in range(n_tx))
    return [
        make_transaction(account, t,
                         random.randint(70_000_000, 99_000_000),
                         account.home_location, channel,
                         i > 0, FraudPattern.ADVANCED_HIGH_FREQUENCY_V1)
        for i, t in enumerate(times)
    ]


def make_advanced_high_frequency_v2(account: AccountProfile, ws: datetime, we: datetime, *, ctx: WindowContext = None) -> list:
    if ctx is None:
        raise ValueError("make_advanced_high_frequency_v2 requires ctx")

    n_tx = random.randint(3, 6)

    night_ws, night_we = ctx.night_window()

    w1 = (ws, we)
    w2 = ctx.next_window()
    idx3 = (ctx.idx + 2) % len(ctx.windows)
    cyc3 = ctx.cycle + ((ctx.idx + 2) // len(ctx.windows))
    w3   = ctx.compute(idx3, cyc3)

    uses_night = any(w_s == night_ws for w_s, _ in [w1, w2, w3])

    if uses_night:
        times = sorted(rand_date(night_ws, night_we) for _ in range(n_tx))
    else:
        span  = (w3[1] - w1[0]).total_seconds()
        times = sorted(
            min(w1[0] + timedelta(seconds=random.uniform(0, span)),
                w3[1] - timedelta(seconds=1))
            for _ in range(n_tx)
        )
        
    high_times: list[datetime] = []
    txs = []

    for i, t in enumerate(times):
        if i < 2:
            amount = random.randint(150_000_000, 300_000_000)
            high_times.append(t)
        else:
            if (t - high_times[0]).total_seconds() > 3600:
                amount = random.randint(150_000_000, 300_000_000)
                high_times.append(t)
            else:
                amount = random.randint(70_000_000, 99_000_000)

        if len(high_times) >= 3:
            high_times.pop(0)

        txs.append(make_transaction(
            account, t, amount,
            account.home_location,
            random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0],
            i > 0, FraudPattern.ADVANCED_HIGH_FREQUENCY_V2,
        ))

    return txs


INJECTOR_MAP = {
    # RULE PATTERNS
    "HIGH_AMOUNT_BLOCK":          make_high_amount_transaction,
    "BLACKLISTED_ACCOUNT":        make_black_list_transaction,

    # CEP PATTERNS
    "HIGH_FREQUENCY":             make_high_frequency,
    "LOCATION_JUMP":              make_location_jump_transaction,
    "DECLINED_BURST":             make_declined_burst_transaction,
    "RAPID_MICROPAYMENTS":        make_rapid_micro_transaction,

    # ML PATTERNS
    "WARM_UP_ACTIVITY":           make_warm_up_activity,
    "MIDNIGHT_ACTIVITY":          make_midnight_activity,
    "UNDER_THREAD_ACTIVITY":      make_under_thread_activity,
    "ADVANCED_LOCATION_JUMP":     make_advanced_location_jump,
    "ADVANCED_HIGH_FREQUENCY_V1": make_advanced_high_frequency_v1,
    "ADVANCED_HIGH_FREQUENCY_V2": make_advanced_high_frequency_v2,
}
