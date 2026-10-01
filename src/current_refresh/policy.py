from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


DEFAULT_MIN_NEW_ROWS = 1000
DEFAULT_MAX_AGE_DAYS = 7


@dataclass(frozen=True)
class RetrainDecision:
    current_rows: int
    last_trained_rows: int | None
    new_rows: int
    days_since_train: float | None
    should_retrain: bool
    reason: str


def decide_retrain(
    *,
    current_rows: int,
    last_trained_rows: int | None,
    last_trained_at: datetime | None,
    now: datetime,
    min_new_rows: int = DEFAULT_MIN_NEW_ROWS,
    max_age_days: int = DEFAULT_MAX_AGE_DAYS,
) -> RetrainDecision:
    if current_rows < 0:
        raise ValueError("current_rows must be >= 0")

    if min_new_rows <= 0:
        raise ValueError("min_new_rows must be > 0")

    if max_age_days <= 0:
        raise ValueError("max_age_days must be > 0")

    now_utc = now.astimezone(timezone.utc)

    if last_trained_rows is None or last_trained_at is None:
        return RetrainDecision(
            current_rows=current_rows,
            last_trained_rows=last_trained_rows,
            new_rows=current_rows,
            days_since_train=None,
            should_retrain=True,
            reason="NO_PREVIOUS_TRAINING",
        )

    if current_rows < last_trained_rows:
        raise RuntimeError(
            "Current Gold row count decreased: "
            f"current={current_rows}, "
            f"last_trained={last_trained_rows}"
        )

    new_rows = current_rows - last_trained_rows

    last_trained_utc = last_trained_at.astimezone(timezone.utc)
    age_seconds = (now_utc - last_trained_utc).total_seconds()

    if age_seconds < 0:
        raise RuntimeError("last_trained_at is in the future")

    days_since_train = age_seconds / 86400.0

    if new_rows == 0:
        return RetrainDecision(
            current_rows=current_rows,
            last_trained_rows=last_trained_rows,
            new_rows=0,
            days_since_train=days_since_train,
            should_retrain=False,
            reason="NO_NEW_ROWS",
        )

    if new_rows >= min_new_rows:
        return RetrainDecision(
            current_rows=current_rows,
            last_trained_rows=last_trained_rows,
            new_rows=new_rows,
            days_since_train=days_since_train,
            should_retrain=True,
            reason="NEW_ROWS_THRESHOLD",
        )

    if days_since_train >= max_age_days:
        return RetrainDecision(
            current_rows=current_rows,
            last_trained_rows=last_trained_rows,
            new_rows=new_rows,
            days_since_train=days_since_train,
            should_retrain=True,
            reason="AGE_THRESHOLD",
        )

    return RetrainDecision(
        current_rows=current_rows,
        last_trained_rows=last_trained_rows,
        new_rows=new_rows,
        days_since_train=days_since_train,
        should_retrain=False,
        reason="BELOW_THRESHOLD",
    )
