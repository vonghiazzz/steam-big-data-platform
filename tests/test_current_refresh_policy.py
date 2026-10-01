from datetime import datetime, timedelta, timezone

import pytest

from src.current_refresh.policy import decide_retrain


NOW = datetime(2026, 10, 2, tzinfo=timezone.utc)


def test_no_new_rows_skips_retrain():
    decision = decide_retrain(
        current_rows=48202,
        last_trained_rows=48202,
        last_trained_at=NOW - timedelta(days=1),
        now=NOW,
    )

    assert decision.should_retrain is False
    assert decision.new_rows == 0
    assert decision.reason == "NO_NEW_ROWS"


def test_small_change_before_seven_days_skips():
    decision = decide_retrain(
        current_rows=49201,
        last_trained_rows=48202,
        last_trained_at=NOW - timedelta(days=2),
        now=NOW,
    )

    assert decision.new_rows == 999
    assert decision.should_retrain is False
    assert decision.reason == "BELOW_THRESHOLD"


def test_one_thousand_new_rows_retrains():
    decision = decide_retrain(
        current_rows=49202,
        last_trained_rows=48202,
        last_trained_at=NOW - timedelta(days=2),
        now=NOW,
    )

    assert decision.new_rows == 1000
    assert decision.should_retrain is True
    assert decision.reason == "NEW_ROWS_THRESHOLD"


def test_seven_days_with_new_data_retrains():
    decision = decide_retrain(
        current_rows=48203,
        last_trained_rows=48202,
        last_trained_at=NOW - timedelta(days=7),
        now=NOW,
    )

    assert decision.new_rows == 1
    assert decision.should_retrain is True
    assert decision.reason == "AGE_THRESHOLD"


def test_row_count_decrease_is_blocked():
    with pytest.raises(RuntimeError):
        decide_retrain(
            current_rows=47000,
            last_trained_rows=48202,
            last_trained_at=NOW - timedelta(days=1),
            now=NOW,
        )


def test_no_previous_training_requires_training():
    decision = decide_retrain(
        current_rows=48202,
        last_trained_rows=None,
        last_trained_at=None,
        now=NOW,
    )

    assert decision.should_retrain is True
    assert decision.reason == "NO_PREVIOUS_TRAINING"
