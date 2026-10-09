"""Synthetic payment producer. No external dataset or service is used."""

from collections import deque
from datetime import datetime, timezone
import json
import logging
import os
import random
import time
import uuid

from kafka import KafkaProducer
from kafka.errors import KafkaError

from .settings import EVENTS_PER_SECOND, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger(__name__)
COUNTRIES = ("US", "CA", "GB", "KR", "JP", "AU", "DE")
MERCHANTS = ("northwind_shop", "metro_travel", "cloud_music", "daily_market", "pixel_goods", "green_grocery")
PAYMENT_METHODS = ("card", "wallet", "bank_transfer")


class EventFactory:
    """Generate stable user profiles plus short bursts that exercise each rule."""

    def __init__(self, seed: int | None = None) -> None:
        self.rng = random.Random(seed)
        self._queued_scenarios: deque[str] = deque()

    def _schedule_scenario(self) -> None:
        scenario = self.rng.choice(("large", "rapid", "declines", "geo"))
        if scenario == "large":
            self._queued_scenarios.extend(("large",))
        elif scenario == "rapid":
            self._queued_scenarios.extend(("rapid",) * 4)
        elif scenario == "declines":
            self._queued_scenarios.extend(("declines",) * 4)
        else:
            self._queued_scenarios.extend(("geo_seed", "geo_shift"))

    def generate(self) -> dict[str, object]:
        if not self._queued_scenarios and self.rng.random() < 0.04:
            self._schedule_scenario()

        scenario = self._queued_scenarios.popleft() if self._queued_scenarios else "normal"
        if scenario in {"rapid", "declines"}:
            user_id = f"scenario_{scenario}_user"
            country = "US"
            status = "declined" if scenario == "declines" else "approved"
            amount = round(self.rng.uniform(12, 180), 2)
        elif scenario in {"geo_seed", "geo_shift"}:
            user_id = "scenario_geo_user"
            country = "US" if scenario == "geo_seed" else "GB"
            status = "approved"
            amount = round(self.rng.uniform(20, 400), 2)
        elif scenario == "large":
            user_number = self.rng.randint(1, 1000)
            user_id = f"user_{user_number:04d}"
            country = COUNTRIES[user_number % len(COUNTRIES)]
            status = "approved"
            amount = round(self.rng.uniform(2500, 7500), 2)
        else:
            user_number = self.rng.randint(1, 1000)
            user_id = f"user_{user_number:04d}"
            country = COUNTRIES[user_number % len(COUNTRIES)]
            status = "declined" if self.rng.random() < 0.06 else "approved"
            amount = round(self.rng.uniform(5, 650), 2)

        return {
            "transaction_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "user_id": user_id,
            "merchant": self.rng.choice(MERCHANTS),
            "amount": amount,
            "payment_method": self.rng.choice(PAYMENT_METHODS),
            "status": status,
            "country": country,
            "device_id": f"device_{user_id}",
        }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=10,
        max_in_flight_requests_per_connection=1,
    )
    factory = EventFactory()
    interval = 1.0 / EVENTS_PER_SECOND
    sent = 0
    LOG.info("publishing to topic=%s at target=%s events/sec", KAFKA_TOPIC, EVENTS_PER_SECOND)
    try:
        while True:
            started = time.monotonic()
            event = factory.generate()
            try:
                delivery = producer.send(KAFKA_TOPIC, key=event["user_id"], value=event)
                delivery.add_errback(lambda exc: LOG.error("Kafka delivery failed: %s", exc))
                sent += 1
                if sent % EVENTS_PER_SECOND == 0:
                    LOG.info("produced %s events", sent)
            except KafkaError:
                LOG.exception("Kafka publish failed")
            time.sleep(max(0.0, interval - (time.monotonic() - started)))
    except KeyboardInterrupt:
        LOG.info("stopping producer")
    finally:
        producer.flush(timeout=10)
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
