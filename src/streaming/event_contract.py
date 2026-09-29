"""Canonical event-envelope helpers shared by streaming producer tests."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping


REVIEW_CREATED = "REVIEW_CREATED"


def utc_iso_from_epoch(value: Any) -> str:
    """Convert a Steam epoch value to a UTC ISO-8601 event time."""
    try:
        epoch = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("timestamp_created must be an integer epoch") from exc
    if epoch <= 0:
        raise ValueError("timestamp_created must be positive")
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


def build_review_created_event(
    appid: int,
    review: Mapping[str, Any],
    *,
    produced_at: str,
) -> dict[str, Any]:
    """Build a deterministic, retry-safe REVIEW_CREATED envelope."""
    recommendationid = str(review.get("recommendationid") or "").strip()
    if not recommendationid:
        raise ValueError("review recommendationid is required")
    if appid <= 0:
        raise ValueError("appid must be positive")

    return {
        "event_id": f"REVIEW_CREATED:{appid}:{recommendationid}",
        "event_type": REVIEW_CREATED,
        "appid": appid,
        "event_time": utc_iso_from_epoch(review.get("timestamp_created")),
        "produced_at": produced_at,
        "payload": dict(review),
    }
