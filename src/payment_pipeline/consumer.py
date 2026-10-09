"""Batched Kafka consumer that enriches events and inserts them into ClickHouse."""

import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import clickhouse_connect
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from kafka.structs import TopicPartition

from .fraud import FraudDetector, process_batch
from .settings import (
    BATCH_POLL_MS,
    BATCH_SIZE,
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_HOST,
    CLICKHOUSE_HTTP_PORT,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER,
    FRAUD_DECLINE_COUNT,
    FRAUD_DECLINE_WINDOW_SECONDS,
    FRAUD_GEO_WINDOW_SECONDS,
    FRAUD_LARGE_PAYMENT_THRESHOLD,
    FRAUD_RAPID_TX_COUNT,
    FRAUD_RAPID_WINDOW_SECONDS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
    KAFKA_TOPIC,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger(__name__)
TABLE = "payments"
INSERT_COLUMNS = [
    "event_time", "transaction_id", "user_id", "merchant", "amount", "payment_method",
    "status", "country", "device_id", "is_suspicious", "fraud_reason", "risk_score",
]


def make_detector() -> FraudDetector:
    return FraudDetector(
        large_payment_threshold=FRAUD_LARGE_PAYMENT_THRESHOLD,
        rapid_window_seconds=FRAUD_RAPID_WINDOW_SECONDS,
        rapid_tx_count=FRAUD_RAPID_TX_COUNT,
        decline_window_seconds=FRAUD_DECLINE_WINDOW_SECONDS,
        decline_count=FRAUD_DECLINE_COUNT,
        geo_window_seconds=FRAUD_GEO_WINDOW_SECONDS,
    )


def to_insert_rows(events: list[dict]) -> list[list]:
    """Map validated/enriched dictionaries to the explicit ClickHouse column order."""
    return [
        [
            event["timestamp"], event["transaction_id"], event["user_id"], event["merchant"],
            event["amount"], event["payment_method"], event["status"], event["country"],
            event["device_id"], int(event["is_suspicious"]), event["fraud_reason"], event["risk_score"],
        ]
        for event in events
    ]


def main() -> None:
    clickhouse = None
    consumer = None
    while clickhouse is None:
        try:
            clickhouse = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_HTTP_PORT,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DATABASE,
            )
            clickhouse.query("SELECT 1")
        except Exception:
            LOG.exception("ClickHouse is not ready; retrying in 3 seconds")
            if clickhouse is not None:
                clickhouse.close()
            clickhouse = None
            time.sleep(3)

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            group_id=KAFKA_GROUP_ID,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=BATCH_SIZE,
            value_deserializer=lambda value: value,
            key_deserializer=lambda key: key.decode("utf-8") if key else None,
        )
        detector = make_detector()
        total_inserted = 0
        LOG.info("consuming topic=%s group=%s batch_size=%s", KAFKA_TOPIC, KAFKA_GROUP_ID, BATCH_SIZE)

        while True:
            polled = consumer.poll(timeout_ms=BATCH_POLL_MS, max_records=BATCH_SIZE)
            messages = [message for records in polled.values() for message in records]
            if not messages:
                continue
            messages.sort(key=lambda message: (message.partition, message.offset))
            state_before_batch = detector.snapshot()
            rows, rejected = process_batch([message.value for message in messages], detector)
            if rejected:
                LOG.warning("rejected %s malformed event(s); raw payloads were not stored", rejected)

            try:
                if rows:
                    clickhouse.insert(TABLE, to_insert_rows(rows), column_names=INSERT_COLUMNS)
                consumer.commit()
                total_inserted += len(rows)
                if total_inserted and total_inserted % (BATCH_SIZE * 10) < len(rows):
                    LOG.info("inserted %s valid events", total_inserted)
            except (Exception, KafkaError):
                LOG.exception("batch insert/commit failed; restoring fraud state and rewinding polled offsets")
                detector.restore(state_before_batch)
                earliest_offsets: dict[TopicPartition, int] = {}
                for message in messages:
                    partition = TopicPartition(message.topic, message.partition)
                    earliest_offsets[partition] = min(earliest_offsets.get(partition, message.offset), message.offset)
                for partition, offset in earliest_offsets.items():
                    consumer.seek(partition, offset)
                time.sleep(1)
    except KeyboardInterrupt:
        LOG.info("stopping consumer")
    finally:
        if consumer is not None:
            consumer.close()
        if clickhouse is not None:
            clickhouse.close()


if __name__ == "__main__":
    main()
