from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_STATE_PATH = (
    PROJECT_ROOT
    / "data"
    / "state"
    / "current_refresh"
    / "current_refresh_v1.json"
)

DEFAULT_LOCK_PATH = (
    PROJECT_ROOT
    / "data"
    / "state"
    / "current_refresh"
    / "current_refresh_v1.lock"
)

DEFAULT_PROFILE_PATH = (
    PROJECT_ROOT
    / "data"
    / "state"
    / "current_refresh"
    / "current_gold_profile.json"
)

DEFAULT_ML_EVIDENCE_ROOT = (
    PROJECT_ROOT
    / "evidence"
    / "ml"
    / "runs"
)

CURRENT_GOLD_INPUT_PATH = "/steam/gold/base_current_v1"


def default_state() -> dict:
    return {
        "schema_version": 1,
        "last_attempt_at": None,
        "last_attempt_status": None,
        "last_error": None,
        "last_current_rows": None,
        "last_analytics_rows": None,
        "last_trained_rows": None,
        "last_trained_at": None,
        "last_ml_run_id": None,
        "last_ml_dataset_fingerprint": None,
    }


def load_state(path: Path = DEFAULT_STATE_PATH) -> dict:
    if not path.exists():
        return default_state()

    try:
        payload = json.loads(
            path.read_text(encoding="utf-8")
        )
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid current refresh state JSON: {path}"
        ) from exc

    if not isinstance(payload, dict):
        raise ValueError(
            f"Current refresh state must be an object: {path}"
        )

    state = default_state()
    state.update(payload)
    return state


def save_state(
    path: Path,
    state: Mapping[str, object],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = (
        path.parent
        / f".{path.name}.{os.getpid()}.tmp"
    )

    temporary.write_text(
        json.dumps(
            dict(state),
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    os.replace(temporary, path)


def parse_run_id(run_id: str) -> datetime:
    parsed = datetime.strptime(
        run_id,
        "%Y%m%dT%H%M%SZ",
    )

    return parsed.replace(
        tzinfo=timezone.utc
    )


def parse_training_summary(path: Path) -> dict:
    values = {}

    for line in path.read_text(
        encoding="utf-8"
    ).splitlines():
        if "=" not in line:
            continue

        key, value = line.split("=", 1)

        if key in {
            "status",
            "run_id",
            "input_path",
            "dataset_fingerprint",
            "source_rows",
        }:
            values[key] = value

    required = {
        "status",
        "run_id",
        "input_path",
        "source_rows",
    }

    missing = required - set(values)

    if missing:
        raise ValueError(
            "Training summary missing fields: "
            + ", ".join(sorted(missing))
        )

    values["source_rows"] = int(
        values["source_rows"]
    )

    return values


def find_latest_current_gold_training(
    evidence_root: Path = DEFAULT_ML_EVIDENCE_ROOT,
) -> dict | None:
    if not evidence_root.exists():
        return None

    run_dirs = sorted(
        (
            path
            for path in evidence_root.iterdir()
            if path.is_dir()
        ),
        key=lambda path: path.name,
        reverse=True,
    )

    for run_dir in run_dirs:
        summary_path = (
            run_dir
            / "training_summary.txt"
        )

        if not summary_path.exists():
            continue

        try:
            summary = parse_training_summary(
                summary_path
            )
        except (ValueError, OSError):
            continue

        if summary["status"] != "PASS":
            continue

        if (
            summary["input_path"]
            != CURRENT_GOLD_INPUT_PATH
        ):
            continue

        run_id = str(summary["run_id"])

        try:
            trained_at = parse_run_id(run_id)
        except ValueError:
            continue

        return {
            "last_trained_rows": int(
                summary["source_rows"]
            ),
            "last_trained_at": (
                trained_at.isoformat()
            ),
            "last_ml_run_id": run_id,
            "last_ml_dataset_fingerprint": (
                summary.get(
                    "dataset_fingerprint"
                )
            ),
        }

    return None


def bootstrap_training_state(
    state: Mapping[str, object],
    evidence_root: Path = DEFAULT_ML_EVIDENCE_ROOT,
) -> dict:
    result = dict(state)

    if (
        result.get("last_trained_rows")
        is not None
        and result.get("last_trained_at")
        is not None
        and result.get("last_ml_run_id")
        is not None
    ):
        return result

    training = (
        find_latest_current_gold_training(
            evidence_root
        )
    )

    if training is None:
        return result

    result.update(training)
    return result
