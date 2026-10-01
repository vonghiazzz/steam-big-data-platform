import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.current_refresh.scheduler import (
    CurrentRefreshScheduler,
)
from src.current_refresh.state import (
    default_state,
    load_state,
    save_state,
)


NOW = datetime(
    2026,
    10,
    2,
    0,
    0,
    tzinfo=timezone.utc,
)


def write_profile(path: Path, rows: int) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": (
                    "2026-10-02T00:00:00+00:00"
                ),
                "historical_path": (
                    "/steam/gold/base"
                ),
                "realtime_path": (
                    "/steam/gold/base_incremental_v1"
                ),
                "current_path": (
                    "/steam/gold/base_current_v1"
                ),
                "historical_rows": 45000,
                "realtime_rows": rows - 45000,
                "overlap_ids": 0,
                "rows": rows,
                "unique_recommendationids": rows,
            }
        )
        + "\n",
        encoding="utf-8",
    )


def write_ml_summary(
    evidence_root: Path,
    run_id: str,
    rows: int,
) -> None:
    run_dir = evidence_root / run_id

    run_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    (
        run_dir
        / "training_summary.txt"
    ).write_text(
        "status=PASS\n"
        f"run_id={run_id}\n"
        "input_path="
        "/steam/gold/base_current_v1\n"
        f"dataset_fingerprint=fp-{run_id}\n"
        f"source_rows={rows}\n",
        encoding="utf-8",
    )


def make_scheduler(
    tmp_path: Path,
    *,
    current_rows: int,
    state=None,
    with_existing_ml=True,
):
    state_path = (
        tmp_path
        / "state.json"
    )

    lock_path = (
        tmp_path
        / "state.lock"
    )

    profile_path = (
        tmp_path
        / "profile.json"
    )

    evidence_root = (
        tmp_path
        / "evidence"
        / "ml"
        / "runs"
    )

    if state is not None:
        save_state(
            state_path,
            state,
        )

    if with_existing_ml:
        write_ml_summary(
            evidence_root,
            "20261001T165508Z",
            48202,
        )

    calls = []

    def runner(command, env):
        script = command[-1]

        calls.append(script)

        if script.endswith(
            "run_current_gold.py"
        ):
            write_profile(
                profile_path,
                current_rows,
            )

        elif script.endswith(
            "run_mllib_v1.py"
        ):
            write_ml_summary(
                evidence_root,
                "20261002T000000Z",
                current_rows,
            )

        return object()

    scheduler = CurrentRefreshScheduler(
        state_path=state_path,
        lock_path=lock_path,
        profile_path=profile_path,
        evidence_root=evidence_root,
        command_runner=runner,
        clock=lambda: NOW,
    )

    return (
        scheduler,
        calls,
        state_path,
        profile_path,
        evidence_root,
    )


def test_first_run_refreshes_analytics_but_skips_ml(
    tmp_path,
):
    (
        scheduler,
        calls,
        state_path,
        _profile,
        _evidence,
    ) = make_scheduler(
        tmp_path,
        current_rows=48202,
    )

    assert scheduler.run_once() == 0

    assert calls == [
        "src/gold/run_current_gold.py",
        "src/analytics/"
        "run_gold_analytics.py",
    ]

    state = load_state(
        state_path
    )

    assert (
        state["last_attempt_status"]
        == "COMPLETED_ANALYTICS_ONLY"
    )

    assert (
        state["last_current_rows"]
        == 48202
    )

    assert (
        state["last_analytics_rows"]
        == 48202
    )

    assert (
        state["last_trained_rows"]
        == 48202
    )

    assert (
        state["last_ml_run_id"]
        == "20261001T165508Z"
    )

    assert (
        state["last_decision_reason"]
        == "NO_NEW_ROWS"
    )


def test_unchanged_rows_skip_analytics_and_ml(
    tmp_path,
):
    state = default_state()

    state.update(
        {
            "last_current_rows": 48202,
            "last_analytics_rows": 48202,
            "last_trained_rows": 48202,
            "last_trained_at": (
                "2026-10-01T16:55:08+00:00"
            ),
            "last_ml_run_id": (
                "20261001T165508Z"
            ),
        }
    )

    (
        scheduler,
        calls,
        state_path,
        _profile,
        _evidence,
    ) = make_scheduler(
        tmp_path,
        current_rows=48202,
        state=state,
    )

    assert scheduler.run_once() == 0

    assert calls == [
        "src/gold/run_current_gold.py",
    ]

    saved = load_state(
        state_path
    )

    assert (
        saved["last_attempt_status"]
        == "COMPLETED_NO_CHANGE"
    )


def test_small_change_refreshes_analytics_only(
    tmp_path,
):
    state = default_state()

    state.update(
        {
            "last_current_rows": 48202,
            "last_analytics_rows": 48202,
            "last_trained_rows": 48202,
            "last_trained_at": (
                "2026-10-01T16:55:08+00:00"
            ),
            "last_ml_run_id": (
                "20261001T165508Z"
            ),
        }
    )

    (
        scheduler,
        calls,
        state_path,
        _profile,
        _evidence,
    ) = make_scheduler(
        tmp_path,
        current_rows=48250,
        state=state,
    )

    assert scheduler.run_once() == 0

    assert calls == [
        "src/gold/run_current_gold.py",
        "src/analytics/"
        "run_gold_analytics.py",
    ]

    saved = load_state(
        state_path
    )

    assert (
        saved["last_attempt_status"]
        == "COMPLETED_ANALYTICS_ONLY"
    )

    assert (
        saved["last_new_rows"]
        == 48
    )

    assert (
        saved["last_decision_reason"]
        == "BELOW_THRESHOLD"
    )


def test_threshold_change_retrains_ml(
    tmp_path,
):
    state = default_state()

    state.update(
        {
            "last_current_rows": 48202,
            "last_analytics_rows": 48202,
            "last_trained_rows": 48202,
            "last_trained_at": (
                "2026-10-01T16:55:08+00:00"
            ),
            "last_ml_run_id": (
                "20261001T165508Z"
            ),
        }
    )

    (
        scheduler,
        calls,
        state_path,
        _profile,
        _evidence,
    ) = make_scheduler(
        tmp_path,
        current_rows=49202,
        state=state,
    )

    assert scheduler.run_once() == 0

    assert calls == [
        "src/gold/run_current_gold.py",
        "src/analytics/"
        "run_gold_analytics.py",
        "src/ml/run_mllib_v1.py",
    ]

    saved = load_state(
        state_path
    )

    assert (
        saved["last_attempt_status"]
        == "COMPLETED_RETRAINED"
    )

    assert (
        saved["last_trained_rows"]
        == 49202
    )

    assert (
        saved["last_ml_run_id"]
        == "20261002T000000Z"
    )


def test_decreasing_current_rows_fails_closed(
    tmp_path,
):
    state = default_state()

    state.update(
        {
            "last_current_rows": 48202,
            "last_analytics_rows": 48202,
            "last_trained_rows": 48202,
            "last_trained_at": (
                "2026-10-01T16:55:08+00:00"
            ),
            "last_ml_run_id": (
                "20261001T165508Z"
            ),
        }
    )

    (
        scheduler,
        calls,
        state_path,
        _profile,
        _evidence,
    ) = make_scheduler(
        tmp_path,
        current_rows=48000,
        state=state,
    )

    with pytest.raises(
        RuntimeError,
        match="row count decreased",
    ):
        scheduler.run_once()

    assert calls == [
        "src/gold/run_current_gold.py",
    ]

    saved = load_state(
        state_path
    )

    assert (
        saved["last_attempt_status"]
        == "FAILED"
    )

    assert (
        "decreased"
        in saved["last_error"]
    )


def test_dry_run_executes_no_commands(
    tmp_path,
):
    (
        scheduler,
        calls,
        _state_path,
        profile_path,
        _evidence,
    ) = make_scheduler(
        tmp_path,
        current_rows=48202,
    )

    write_profile(
        profile_path,
        48202,
    )

    assert (
        scheduler.run_once(
            dry_run=True
        )
        == 0
    )

    assert calls == []
