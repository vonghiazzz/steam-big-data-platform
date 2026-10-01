from datetime import timezone

from src.current_refresh.state import (
    bootstrap_training_state,
    default_state,
    find_latest_current_gold_training,
    load_state,
    parse_run_id,
    save_state,
)


def write_summary(
    root,
    run_id,
    *,
    input_path="/steam/gold/base_current_v1",
    rows=48202,
    status="PASS",
    fingerprint="abc123",
):
    run_dir = root / run_id
    run_dir.mkdir(parents=True)

    (run_dir / "training_summary.txt").write_text(
        f"status={status}\n"
        f"run_id={run_id}\n"
        f"input_path={input_path}\n"
        f"dataset_fingerprint={fingerprint}\n"
        f"source_rows={rows}\n",
        encoding="utf-8",
    )


def test_missing_state_returns_defaults(tmp_path):
    state = load_state(
        tmp_path / "missing.json"
    )

    assert state["schema_version"] == 1
    assert state["last_trained_rows"] is None


def test_state_round_trip(tmp_path):
    path = tmp_path / "state.json"

    state = default_state()
    state["last_current_rows"] = 48202

    save_state(path, state)

    loaded = load_state(path)

    assert loaded["last_current_rows"] == 48202


def test_parse_run_id_is_utc():
    parsed = parse_run_id(
        "20261001T165508Z"
    )

    assert parsed.tzinfo == timezone.utc
    assert parsed.year == 2026
    assert parsed.month == 10
    assert parsed.day == 1


def test_finds_latest_current_gold_run(tmp_path):
    write_summary(
        tmp_path,
        "20261001T164858Z",
        rows=48100,
    )

    write_summary(
        tmp_path,
        "20261001T165508Z",
        rows=48202,
        fingerprint="latest123",
    )

    latest = (
        find_latest_current_gold_training(
            tmp_path
        )
    )

    assert latest is not None
    assert latest["last_trained_rows"] == 48202
    assert (
        latest["last_ml_run_id"]
        == "20261001T165508Z"
    )
    assert (
        latest["last_ml_dataset_fingerprint"]
        == "latest123"
    )


def test_ignores_non_current_gold_run(tmp_path):
    write_summary(
        tmp_path,
        "20261001T170000Z",
        input_path="/steam/gold/base",
    )

    assert (
        find_latest_current_gold_training(
            tmp_path
        )
        is None
    )


def test_bootstrap_existing_current_ml_run(tmp_path):
    write_summary(
        tmp_path,
        "20261001T165508Z",
        rows=48202,
    )

    state = bootstrap_training_state(
        default_state(),
        tmp_path,
    )

    assert state["last_trained_rows"] == 48202
    assert (
        state["last_ml_run_id"]
        == "20261001T165508Z"
    )
    assert (
        state["last_trained_at"]
        == "2026-10-01T16:55:08+00:00"
    )


def test_existing_training_state_is_preserved(
    tmp_path,
):
    write_summary(
        tmp_path,
        "20261001T170000Z",
        rows=50000,
    )

    state = default_state()
    state["last_trained_rows"] = 48202
    state["last_trained_at"] = (
        "2026-10-01T16:55:08+00:00"
    )
    state["last_ml_run_id"] = (
        "20261001T165508Z"
    )

    result = bootstrap_training_state(
        state,
        tmp_path,
    )

    assert result["last_trained_rows"] == 48202
    assert (
        result["last_ml_run_id"]
        == "20261001T165508Z"
    )
