CREATE TABLE IF NOT EXISTS fraud_detection.transactions
(
    transaction_id String,
    account_id     String,

    amount         Decimal(18, 2),

    rule_hit       Nullable(String),
    cep_pattern    Nullable(String),
    ml_score       Nullable(Float32),

    produced_at    DateTime64(3),
    decided_at     DateTime64(3),

    decision       Enum8('APPROVED' = 1, 'ALERT' = 2, 'BLOCK' = 3)
)
ENGINE = MergeTree()
ORDER BY produced_at;
