"""Canonical event-envelope helpers shared by streaming producer tests."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping


REVIEW_CREATED = "REVIEW_CREATED"
PLAYER_COUNT_SNAPSHOT = "PLAYER_COUNT_SNAPSHOT"
PLAYER_COUNT_SCHEMA_VERSION = 1


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


def build_player_count_snapshot_event(
    appid: int,
    player_count: int,
    *,
    observed_at: str,
    produced_at: str,
) -> dict[str, Any]:
    """Build a deterministic player-count observation event."""
    if isinstance(appid, bool) or not isinstance(appid, int) or appid <= 0:
        raise ValueError("appid must be a positive integer")
    if (
        isinstance(player_count, bool)
        or not isinstance(player_count, int)
        or player_count < 0
    ):
        raise ValueError("player_count must be a non-negative integer")

    observed = _normalized_utc_iso(observed_at, "observed_at")
    produced = _normalized_utc_iso(produced_at, "produced_at")
    return {
        "event_id": f"{PLAYER_COUNT_SNAPSHOT}:{appid}:{observed}",
        "event_type": PLAYER_COUNT_SNAPSHOT,
        "schema_version": PLAYER_COUNT_SCHEMA_VERSION,
        "appid": appid,
        "event_time": observed,
        "produced_at": produced,
        "payload": {
            "player_count": player_count,
        },
    }


def _normalized_utc_iso(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat()
