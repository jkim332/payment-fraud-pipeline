"""Event schema validation and normalization."""

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import math
import re
from typing import Any

REQUIRED_FIELDS = {
    "transaction_id",
    "timestamp",
    "user_id",
    "merchant",
    "amount",
    "payment_method",
    "status",
    "country",
    "device_id",
}
ALLOWED_STATUSES = {"approved", "declined"}
ALLOWED_PAYMENT_METHODS = {"card", "wallet", "bank_transfer"}
COUNTRY_CODE = re.compile(r"^[A-Z]{2}$")


class EventValidationError(ValueError):
    """Raised when a payment event does not match the expected contract."""


def validate_event(raw: Any) -> dict[str, Any]:
    """Return a normalized event or raise a useful validation error."""
    if not isinstance(raw, dict):
        raise EventValidationError("event must be a JSON object")

    missing = REQUIRED_FIELDS - raw.keys()
    if missing:
        raise EventValidationError(f"missing required fields: {', '.join(sorted(missing))}")

    event = dict(raw)
    for name in ("transaction_id", "user_id", "merchant", "payment_method", "status", "country", "device_id"):
        if not isinstance(event[name], str) or not event[name].strip():
            raise EventValidationError(f"{name} must be a non-empty string")
        event[name] = event[name].strip()

    event["status"] = event["status"].lower()
    event["payment_method"] = event["payment_method"].lower()
    event["country"] = event["country"].upper()
    if event["status"] not in ALLOWED_STATUSES:
        raise EventValidationError(f"status must be one of {sorted(ALLOWED_STATUSES)}")
    if event["payment_method"] not in ALLOWED_PAYMENT_METHODS:
        raise EventValidationError(f"payment_method must be one of {sorted(ALLOWED_PAYMENT_METHODS)}")
    if not COUNTRY_CODE.fullmatch(event["country"]):
        raise EventValidationError("country must be a two-letter ISO-style code")

    try:
        amount = Decimal(str(event["amount"]))
    except (InvalidOperation, ValueError):
        raise EventValidationError("amount must be numeric") from None
    if isinstance(event["amount"], bool) or not amount.is_finite() or amount < 0:
        raise EventValidationError("amount must be a finite non-negative number")
    # Keep values inside Decimal(12, 2), so validation prevents retry loops on DB overflow.
    if amount > Decimal("9999999999.99"):
        raise EventValidationError("amount exceeds the supported maximum of 9999999999.99")
    try:
        event["amount"] = amount.quantize(Decimal("0.01"))
    except InvalidOperation:
        raise EventValidationError("amount cannot be represented with two decimal places") from None

    try:
        timestamp_text = event["timestamp"]
        if not isinstance(timestamp_text, str):
            raise ValueError
        parsed = datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise EventValidationError("timestamp must include a timezone")
        event["timestamp"] = parsed.astimezone(timezone.utc)
    except EventValidationError:
        raise
    except (ValueError, TypeError, OverflowError):
        raise EventValidationError("timestamp must be an ISO-8601 datetime with timezone") from None

    return event
