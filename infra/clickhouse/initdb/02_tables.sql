CREATE TABLE IF NOT EXISTS fraud_detection.transactions
(
    transaction_id String,
    account_id     String,

    amount         Decimal(18, 2),

    rule_hit       Nullable(String),
    cep_pattern    Nullable(String),
    ml_score       Nullable(Float32),

    produced_at    DateTime64(6, 'UTC'),
    decided_at     DateTime64(6, 'UTC'),

    decision       Enum8('APPROVED' = 1, 'ALERT' = 2, 'BLOCK' = 3)
)
ENGINE = MergeTree()
ORDER BY produced_at;

CREATE TABLE IF NOT EXISTS fraud_detection.accounts
(
    account_id     String,
    card_id        String,
    home_location  String,
    avg_amount     UInt64
)
ENGINE = MergeTree()
ORDER BY account_id;

CREATE TABLE IF NOT EXISTS fraud_detection.ground_true_transactions
(
    transaction_id String,
    produced_at    DateTime64(6, 'UTC'),
    event_time     DateTime64(6, 'UTC'),
    account_id     String,
    card_id        String,
    amount         UInt64,
    channel        String,
    location_name  String,
    status         String,
    is_fraud       Bool,
    fraud_pattern  String
)
ENGINE = MergeTree()
ORDER BY (event_time, transaction_id);
