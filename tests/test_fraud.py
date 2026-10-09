from datetime import datetime, timedelta, timezone
from decimal import Decimal

from payment_pipeline.fraud import FraudDetector

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def make_event(seconds=0, **overrides):
    event = {
        "transaction_id": f"txn-{seconds}-{overrides.get('user_id', 'u1')}",
        "timestamp": BASE + timedelta(seconds=seconds),
        "user_id": "u1",
        "merchant": "shop",
        "amount": Decimal("20.00"),
        "payment_method": "card",
        "status": "approved",
        "country": "US",
        "device_id": "device-1",
    }
    event.update(overrides)
    return event


def test_large_payment_threshold_includes_boundary():
    detector = FraudDetector(large_payment_threshold=100)
    below = detector.score(make_event(amount=Decimal("99.99")))
    at = detector.score(make_event(seconds=1, amount=Decimal("100.00")))
    assert below["fraud_reason"] == []
    assert at["fraud_reason"] == ["large_payment"]
    assert at["risk_score"] == 45


def test_rapid_rule_triggers_on_configured_transaction_count():
    detector = FraudDetector(rapid_window_seconds=60, rapid_tx_count=4)
    scored = [detector.score(make_event(seconds=i * 5)) for i in range(4)]
    assert all("rapid_transactions" not in row["fraud_reason"] for row in scored[:3])
    assert "rapid_transactions" in scored[3]["fraud_reason"]


def test_rapid_rule_does_not_trigger_when_transactions_are_spread_out():
    detector = FraudDetector(rapid_window_seconds=60, rapid_tx_count=4)
    scored = [detector.score(make_event(seconds=i * 61)) for i in range(4)]
    assert all("rapid_transactions" not in row["fraud_reason"] for row in scored)


def test_repeated_declines_trigger_on_fourth_decline():
    detector = FraudDetector(decline_window_seconds=600, decline_count=4)
    scored = [detector.score(make_event(seconds=i * 10, status="declined")) for i in range(4)]
    assert all("repeated_declines" not in row["fraud_reason"] for row in scored[:3])
    assert "repeated_declines" in scored[3]["fraud_reason"]


def test_geo_rule_detects_country_change_inside_window():
    detector = FraudDetector(geo_window_seconds=900)
    detector.score(make_event(seconds=0, country="US"))
    shifted = detector.score(make_event(seconds=5, country="GB"))
    assert "geo_mismatch" in shifted["fraud_reason"]


def test_geo_rule_ignores_country_change_outside_window():
    detector = FraudDetector(geo_window_seconds=900)
    detector.score(make_event(seconds=0, country="US"))
    shifted = detector.score(make_event(seconds=901, country="GB"))
    assert "geo_mismatch" not in shifted["fraud_reason"]


def test_risk_score_is_capped_at_100():
    detector = FraudDetector(large_payment_threshold=100, rapid_window_seconds=60, rapid_tx_count=4)
    for second in (0, 1, 2):
        detector.score(make_event(seconds=second))
    result = detector.score(make_event(seconds=3, amount=Decimal("500"), country="GB"))
    assert {"large_payment", "rapid_transactions", "geo_mismatch"}.issubset(result["fraud_reason"])
    assert result["risk_score"] == 100


def test_detector_state_can_be_restored_after_failed_batch():
    detector = FraudDetector(rapid_tx_count=3)
    detector.score(make_event(seconds=0))
    snapshot = detector.snapshot()
    detector.score(make_event(seconds=1))
    detector.restore(snapshot)
    third = detector.score(make_event(seconds=2))
    assert "rapid_transactions" not in third["fraud_reason"]
