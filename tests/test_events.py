from datetime import datetime, timezone
from decimal import Decimal

import pytest

from payment_pipeline.events import EventValidationError, validate_event


def event(**overrides):
    base = {
        "transaction_id": "txn-1",
        "timestamp": "2026-01-01T00:00:00Z",
        "user_id": "user-1",
        "merchant": "shop",
        "amount": 19.99,
        "payment_method": "card",
        "status": "approved",
        "country": "us",
        "device_id": "device-1",
    }
    base.update(overrides)
    return base


def test_valid_event_is_normalized():
    normalized = validate_event(event())
    assert normalized["amount"] == Decimal("19.99")
    assert normalized["country"] == "US"
    assert normalized["timestamp"] == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_missing_required_field_is_rejected():
    raw = event()
    del raw["merchant"]
    with pytest.raises(EventValidationError, match="merchant"):
        validate_event(raw)


@pytest.mark.parametrize("amount", [-1, True, "NaN", "not-money", "1e100"])
def test_bad_amount_is_rejected(amount):
    with pytest.raises(EventValidationError):
        validate_event(event(amount=amount))


def test_timestamp_must_have_timezone():
    with pytest.raises(EventValidationError, match="timezone"):
        validate_event(event(timestamp="2026-01-01T00:00:00"))


def test_unknown_status_is_rejected():
    with pytest.raises(EventValidationError, match="status"):
        validate_event(event(status="pending"))
