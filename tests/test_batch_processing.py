import json

from payment_pipeline.fraud import FraudDetector, process_batch


def payload(transaction_id="txn-1", amount=25):
    return json.dumps({
        "transaction_id": transaction_id,
        "timestamp": "2026-01-01T00:00:00Z",
        "user_id": "user-1",
        "merchant": "shop",
        "amount": amount,
        "payment_method": "card",
        "status": "approved",
        "country": "US",
        "device_id": "device-1",
    }).encode()


def test_batch_keeps_valid_rows_and_counts_invalid_payloads():
    detector = FraudDetector()
    rows, rejected = process_batch([payload("good-1"), b"not-json", payload("bad-amount", -10)], detector)
    assert len(rows) == 1
    assert rejected == 2
    assert rows[0]["transaction_id"] == "good-1"
    assert rows[0]["is_suspicious"] is False
    assert rows[0]["fraud_reason"] == []


def test_batch_applies_stateful_rules_in_input_order():
    detector = FraudDetector(rapid_tx_count=3)
    rows, rejected = process_batch([payload("txn-1"), payload("txn-2"), payload("txn-3")], detector)
    assert rejected == 0
    assert "rapid_transactions" not in rows[0]["fraud_reason"]
    assert "rapid_transactions" not in rows[1]["fraud_reason"]
    assert "rapid_transactions" in rows[2]["fraud_reason"]
