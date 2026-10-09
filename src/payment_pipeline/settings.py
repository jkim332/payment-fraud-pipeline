"""Environment-backed settings shared by the producer and consumer."""

import os


def env_int(name: str, default: int, minimum: int = 1) -> int:
    value = int(os.getenv(name, str(default)))
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "payments")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "payment-analytics")
EVENTS_PER_SECOND = env_int("EVENTS_PER_SECOND", 100)
BATCH_SIZE = env_int("BATCH_SIZE", 500)
BATCH_POLL_MS = env_int("BATCH_POLL_MS", 1000)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_HTTP_PORT = env_int("CLICKHOUSE_HTTP_PORT", 8123)
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "payments")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "payments_app")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "change-me-local")

FRAUD_LARGE_PAYMENT_THRESHOLD = float(os.getenv("FRAUD_LARGE_PAYMENT_THRESHOLD", "2500"))
FRAUD_RAPID_WINDOW_SECONDS = env_int("FRAUD_RAPID_WINDOW_SECONDS", 60)
FRAUD_RAPID_TX_COUNT = env_int("FRAUD_RAPID_TX_COUNT", 4)
FRAUD_DECLINE_WINDOW_SECONDS = env_int("FRAUD_DECLINE_WINDOW_SECONDS", 600)
FRAUD_DECLINE_COUNT = env_int("FRAUD_DECLINE_COUNT", 4)
FRAUD_GEO_WINDOW_SECONDS = env_int("FRAUD_GEO_WINDOW_SECONDS", 900)
