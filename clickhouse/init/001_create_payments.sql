CREATE DATABASE IF NOT EXISTS payments;

CREATE TABLE IF NOT EXISTS payments.payments
(
    event_time      DateTime64(3, 'UTC'),
    event_date      Date MATERIALIZED toDate(event_time),
    transaction_id  String,
    user_id         String,
    merchant        LowCardinality(String),
    amount          Decimal(12, 2),
    payment_method  LowCardinality(String),
    status          LowCardinality(String),
    country         LowCardinality(String),
    device_id       String,
    is_suspicious   UInt8,
    fraud_reason    Array(String),
    risk_score      UInt8,
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_date, transaction_id);
