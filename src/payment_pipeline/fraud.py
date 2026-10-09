"""Small, explainable online fraud rules with bounded per-user state."""

from collections import defaultdict, deque
from copy import deepcopy
from datetime import datetime
from typing import Any


class FraudDetector:
    def __init__(
        self,
        large_payment_threshold: float = 2500.0,
        rapid_window_seconds: int = 60,
        rapid_tx_count: int = 4,
        decline_window_seconds: int = 600,
        decline_count: int = 4,
        geo_window_seconds: int = 900,
    ) -> None:
        if large_payment_threshold < 0:
            raise ValueError("large_payment_threshold must be non-negative")
        if rapid_tx_count < 2 or decline_count < 2:
            raise ValueError("transaction counts must be at least 2")
        self.large_payment_threshold = large_payment_threshold
        self.rapid_window_seconds = rapid_window_seconds
        self.rapid_tx_count = rapid_tx_count
        self.decline_window_seconds = decline_window_seconds
        self.decline_count = decline_count
        self.geo_window_seconds = geo_window_seconds
        self._transactions: dict[str, deque[datetime]] = defaultdict(deque)
        self._declines: dict[str, deque[datetime]] = defaultdict(deque)
        self._last_location: dict[str, tuple[str, datetime]] = {}

    def snapshot(self) -> tuple[Any, Any, Any]:
        """Capture state so a failed database batch can be retried cleanly."""
        return deepcopy(self._transactions), deepcopy(self._declines), deepcopy(self._last_location)

    def restore(self, snapshot: tuple[Any, Any, Any]) -> None:
        self._transactions, self._declines, self._last_location = deepcopy(snapshot)

    @staticmethod
    def _prune(history: deque[datetime], now: datetime, window_seconds: int) -> None:
        while history and (now - history[0]).total_seconds() > window_seconds:
            history.popleft()

    def score(self, event: dict[str, Any]) -> dict[str, Any]:
        """Score one already-validated event and update the user's rolling state."""
        user_id = event["user_id"]
        now = event["timestamp"]
        reasons: list[str] = []
        weights = {
            "large_payment": 45,
            "rapid_transactions": 30,
            "repeated_declines": 35,
            "geo_mismatch": 40,
        }

        if event["amount"] >= self.large_payment_threshold:
            reasons.append("large_payment")

        transactions = self._transactions[user_id]
        self._prune(transactions, now, self.rapid_window_seconds)
        if len(transactions) >= self.rapid_tx_count - 1:
            reasons.append("rapid_transactions")
        transactions.append(now)

        declines = self._declines[user_id]
        self._prune(declines, now, self.decline_window_seconds)
        if event["status"] == "declined":
            if len(declines) >= self.decline_count - 1:
                reasons.append("repeated_declines")
            declines.append(now)

        previous_location = self._last_location.get(user_id)
        if previous_location:
            previous_country, previous_time = previous_location
            age = (now - previous_time).total_seconds()
            if 0 <= age <= self.geo_window_seconds and previous_country != event["country"]:
                reasons.append("geo_mismatch")
        self._last_location[user_id] = (event["country"], now)

        event["is_suspicious"] = bool(reasons)
        event["fraud_reason"] = reasons
        event["risk_score"] = min(100, sum(weights[reason] for reason in reasons))
        return event


def process_batch(payloads: list[bytes | str], detector: FraudDetector) -> tuple[list[dict[str, Any]], int]:
    """Decode, validate, and enrich a batch; return rows and rejected count."""
    import json
    from .events import EventValidationError, validate_event

    rows: list[dict[str, Any]] = []
    rejected = 0
    for payload in payloads:
        try:
            raw = json.loads(payload)
            event = validate_event(raw)
        except (json.JSONDecodeError, UnicodeDecodeError, EventValidationError, TypeError) as exc:
            rejected += 1
            # The consumer logs a count rather than writing malformed payloads to analytics.
            continue
        rows.append(detector.score(event))
    return rows, rejected
