from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator, Mapping

from src.current_refresh.policy import decide_retrain
from src.current_refresh.state import (
    CURRENT_GOLD_INPUT_PATH,
    DEFAULT_LOCK_PATH,
    DEFAULT_ML_EVIDENCE_ROOT,
    DEFAULT_PROFILE_PATH,
    DEFAULT_STATE_PATH,
    PROJECT_ROOT,
    bootstrap_training_state,
    find_latest_current_gold_training,
    load_state,
    save_state,
)


DEFAULT_CHECK_INTERVAL_SECONDS = 3600
DEFAULT_MIN_NEW_ROWS = 1000
DEFAULT_MAX_AGE_DAYS = 7

DEFAULT_ANALYTICS_ROOT = (
    "/steam/gold/analytics_current_v1"
)

DEFAULT_MODEL_ROOT = (
    "/steam/models/mllib/v1"
)

DEFAULT_PREDICTIONS_ROOT = (
    "/steam/ml/mllib/v1/test_predictions"
)

CommandRunner = Callable[
    [list[str], Mapping[str, str]],
    object,
]


class CurrentRefreshBusyError(RuntimeError):
    """Raised when another refresh owns the lock."""


def load_current_profile(path: Path) -> dict:
    if not path.exists():
        raise RuntimeError(
            f"Current Gold profile not found: {path}"
        )

    try:
        payload = json.loads(
            path.read_text(encoding="utf-8")
        )
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid Current Gold profile: {path}"
        ) from exc

    if not isinstance(payload, dict):
        raise ValueError(
            "Current Gold profile must be an object"
        )

    required = {
        "rows",
        "unique_recommendationids",
        "current_path",
    }

    missing = required - set(payload)

    if missing:
        raise ValueError(
            "Current Gold profile missing fields: "
            + ", ".join(sorted(missing))
        )

    rows = int(payload["rows"])
    unique_ids = int(
        payload["unique_recommendationids"]
    )

    if rows <= 0:
        raise RuntimeError(
            "Current Gold profile contains no rows"
        )

    if rows != unique_ids:
        raise RuntimeError(
            "Current Gold profile is not unique: "
            f"rows={rows}, "
            f"unique={unique_ids}"
        )

    if (
        payload["current_path"]
        != CURRENT_GOLD_INPUT_PATH
    ):
        raise RuntimeError(
            "Unexpected Current Gold path: "
            f"{payload['current_path']}"
        )

    return payload


def parse_iso_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)

    if parsed.tzinfo is None:
        raise ValueError(
            "Datetime must include timezone"
        )

    return parsed.astimezone(timezone.utc)


class CurrentRefreshScheduler:
    def __init__(
        self,
        *,
        state_path: Path = DEFAULT_STATE_PATH,
        lock_path: Path = DEFAULT_LOCK_PATH,
        profile_path: Path = DEFAULT_PROFILE_PATH,
        evidence_root: Path = DEFAULT_ML_EVIDENCE_ROOT,
        command_runner: CommandRunner | None = None,
        clock: Callable[[], datetime] | None = None,
        min_new_rows: int = DEFAULT_MIN_NEW_ROWS,
        max_age_days: int = DEFAULT_MAX_AGE_DAYS,
    ) -> None:
        self.state_path = state_path
        self.lock_path = lock_path
        self.profile_path = profile_path
        self.evidence_root = evidence_root

        self.command_runner = (
            command_runner
            or self._run_command
        )

        self.clock = (
            clock
            or (
                lambda: datetime.now(
                    timezone.utc
                )
            )
        )

        self.min_new_rows = min_new_rows
        self.max_age_days = max_age_days

    def run_once(
        self,
        *,
        dry_run: bool = False,
    ) -> int:
        with self._scheduler_lock():
            state = bootstrap_training_state(
                load_state(self.state_path),
                self.evidence_root,
            )

            if dry_run:
                return self._dry_run(state)

            attempted_at = (
                self.clock()
                .astimezone(timezone.utc)
                .isoformat()
            )

            state.update(
                {
                    "last_attempt_at": attempted_at,
                    "last_attempt_status": "RUNNING",
                    "last_error": None,
                }
            )

            save_state(
                self.state_path,
                state,
            )

            try:
                self._run_current_gold()

                profile = load_current_profile(
                    self.profile_path
                )

                current_rows = int(
                    profile["rows"]
                )

                previous_current_rows = (
                    state.get(
                        "last_current_rows"
                    )
                )

                if (
                    previous_current_rows
                    is not None
                    and current_rows
                    < int(previous_current_rows)
                ):
                    raise RuntimeError(
                        "Current Gold row count "
                        "decreased: "
                        f"previous="
                        f"{previous_current_rows}, "
                        f"current={current_rows}"
                    )

                analytics_needed = (
                    state.get(
                        "last_analytics_rows"
                    )
                    != current_rows
                )

                if analytics_needed:
                    self._run_current_analytics()

                last_trained_at_raw = (
                    state.get(
                        "last_trained_at"
                    )
                )

                last_trained_at = (
                    parse_iso_datetime(
                        str(
                            last_trained_at_raw
                        )
                    )
                    if last_trained_at_raw
                    else None
                )

                decision = decide_retrain(
                    current_rows=current_rows,
                    last_trained_rows=(
                        state.get(
                            "last_trained_rows"
                        )
                    ),
                    last_trained_at=(
                        last_trained_at
                    ),
                    now=self.clock(),
                    min_new_rows=(
                        self.min_new_rows
                    ),
                    max_age_days=(
                        self.max_age_days
                    ),
                )

                old_ml_run_id = (
                    state.get(
                        "last_ml_run_id"
                    )
                )

                retrained = False

                if decision.should_retrain:
                    self._run_ml()

                    latest = (
                        find_latest_current_gold_training(
                            self.evidence_root
                        )
                    )

                    if latest is None:
                        raise RuntimeError(
                            "ML completed but no PASS "
                            "Current Gold training "
                            "evidence was found"
                        )

                    if (
                        latest[
                            "last_ml_run_id"
                        ]
                        == old_ml_run_id
                    ):
                        raise RuntimeError(
                            "ML completed but no new "
                            "versioned run was found"
                        )

                    if (
                        int(
                            latest[
                                "last_trained_rows"
                            ]
                        )
                        != current_rows
                    ):
                        raise RuntimeError(
                            "New ML run row count "
                            "does not match "
                            "Current Gold: "
                            f"ml="
                            f"{latest['last_trained_rows']}, "
                            f"current={current_rows}"
                        )

                    state.update(latest)
                    retrained = True

                state[
                    "last_current_rows"
                ] = current_rows

                if analytics_needed:
                    state[
                        "last_analytics_rows"
                    ] = current_rows

                state[
                    "last_decision_reason"
                ] = decision.reason

                state[
                    "last_new_rows"
                ] = decision.new_rows

                state[
                    "last_successful_at"
                ] = (
                    self.clock()
                    .astimezone(timezone.utc)
                    .isoformat()
                )

                if retrained:
                    status = (
                        "COMPLETED_RETRAINED"
                    )
                elif analytics_needed:
                    status = (
                        "COMPLETED_ANALYTICS_ONLY"
                    )
                else:
                    status = (
                        "COMPLETED_NO_CHANGE"
                    )

                state[
                    "last_attempt_status"
                ] = status

                state["last_error"] = None

                save_state(
                    self.state_path,
                    state,
                )

                result = {
                    "status": status,
                    "current_rows": (
                        current_rows
                    ),
                    "analytics_refreshed": (
                        analytics_needed
                    ),
                    "ml_retrained": retrained,
                    "decision": decision.reason,
                    "new_rows_since_training": (
                        decision.new_rows
                    ),
                    "last_ml_run_id": (
                        state.get(
                            "last_ml_run_id"
                        )
                    ),
                }

                print(
                    json.dumps(
                        result,
                        indent=2,
                        ensure_ascii=False,
                    )
                )

                return 0

            except Exception as exc:
                state[
                    "last_attempt_status"
                ] = "FAILED"

                state[
                    "last_error"
                ] = str(exc)

                save_state(
                    self.state_path,
                    state,
                )

                raise

    def _dry_run(
        self,
        state: Mapping[str, object],
    ) -> int:
        if not self.profile_path.exists():
            print(
                json.dumps(
                    {
                        "dry_run": True,
                        "action": (
                            "CURRENT_PROFILE_UNAVAILABLE"
                        ),
                        "note": (
                            "Dry run does not execute "
                            "Spark jobs."
                        ),
                    },
                    indent=2,
                )
            )

            return 0

        profile = load_current_profile(
            self.profile_path
        )

        current_rows = int(
            profile["rows"]
        )

        last_trained_at_raw = (
            state.get(
                "last_trained_at"
            )
        )

        last_trained_at = (
            parse_iso_datetime(
                str(last_trained_at_raw)
            )
            if last_trained_at_raw
            else None
        )

        decision = decide_retrain(
            current_rows=current_rows,
            last_trained_rows=(
                state.get(
                    "last_trained_rows"
                )
            ),
            last_trained_at=last_trained_at,
            now=self.clock(),
            min_new_rows=(
                self.min_new_rows
            ),
            max_age_days=(
                self.max_age_days
            ),
        )

        payload = {
            "dry_run": True,
            "profile_rows": current_rows,
            "last_analytics_rows": (
                state.get(
                    "last_analytics_rows"
                )
            ),
            "last_trained_rows": (
                state.get(
                    "last_trained_rows"
                )
            ),
            "last_ml_run_id": (
                state.get(
                    "last_ml_run_id"
                )
            ),
            "analytics_needed": (
                state.get(
                    "last_analytics_rows"
                )
                != current_rows
            ),
            "ml_should_retrain": (
                decision.should_retrain
            ),
            "decision": decision.reason,
            "new_rows_since_training": (
                decision.new_rows
            ),
        }

        print(
            json.dumps(
                payload,
                indent=2,
                ensure_ascii=False,
            )
        )

        return 0

    def _base_env(self) -> dict[str, str]:
        env = dict(os.environ)

        project_paths = (
            f"{PROJECT_ROOT}:"
            f"{PROJECT_ROOT / 'src'}"
        )

        existing_pythonpath = (
            env.get("PYTHONPATH")
        )

        env["PYTHONPATH"] = (
            project_paths
            + (
                f":{existing_pythonpath}"
                if existing_pythonpath
                else ""
            )
        )

        venv_python = (
            PROJECT_ROOT
            / ".venv"
            / "bin"
            / "python"
        )

        if venv_python.exists():
            env.setdefault(
                "PYSPARK_PYTHON",
                str(venv_python),
            )

            env.setdefault(
                "PYSPARK_DRIVER_PYTHON",
                str(venv_python),
            )

        return env

    def _spark_command(
        self,
        script: str,
    ) -> list[str]:
        env = os.environ

        command = [
            env.get(
                "SPARK_SUBMIT_BIN",
                "spark-submit",
            ),
            "--master",
            env.get(
                "CURRENT_REFRESH_SPARK_MASTER",
                "local[2]",
            ),
        ]

        hdfs_default_fs = env.get(
            "HDFS_DEFAULT_FS"
        )

        if hdfs_default_fs:
            command.extend(
                [
                    "--conf",
                    (
                        "spark.hadoop.fs.defaultFS="
                        f"{hdfs_default_fs}"
                    ),
                ]
            )

        command.append(script)

        return command

    def _run_current_gold(self) -> None:
        env = self._base_env()

        env[
            "CURRENT_GOLD_PROFILE_PATH"
        ] = str(self.profile_path)

        self._invoke(
            self._spark_command(
                "src/gold/run_current_gold.py"
            ),
            env,
        )

    def _run_current_analytics(
        self,
    ) -> None:
        env = self._base_env()

        env[
            "GOLD_ANALYTICS_INPUT_PATH"
        ] = CURRENT_GOLD_INPUT_PATH

        env[
            "GOLD_ANALYTICS_ROOT"
        ] = os.getenv(
            "CURRENT_ANALYTICS_ROOT",
            DEFAULT_ANALYTICS_ROOT,
        )

        self._invoke(
            self._spark_command(
                "src/analytics/"
                "run_gold_analytics.py"
            ),
            env,
        )

    def _run_ml(self) -> None:
        env = self._base_env()

        env[
            "ML_INPUT_PATH"
        ] = CURRENT_GOLD_INPUT_PATH

        env[
            "ML_MODEL_ROOT"
        ] = os.getenv(
            "ML_MODEL_ROOT",
            DEFAULT_MODEL_ROOT,
        )

        env[
            "ML_PREDICTIONS_PATH"
        ] = os.getenv(
            "ML_PREDICTIONS_PATH",
            DEFAULT_PREDICTIONS_ROOT,
        )

        env[
            "ML_EVIDENCE_DIR"
        ] = str(
            self.evidence_root.parent
        )

        env.setdefault(
            "SPARK_LOG_LEVEL",
            "WARN",
        )

        self._invoke(
            self._spark_command(
                "src/ml/run_mllib_v1.py"
            ),
            env,
        )

    def _invoke(
        self,
        command: list[str],
        env: Mapping[str, str],
    ) -> None:
        self.command_runner(
            command,
            env,
        )

    @staticmethod
    def _run_command(
        command: list[str],
        env: Mapping[str, str],
    ) -> object:
        print(
            "RUN:",
            " ".join(command),
        )

        return subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            env=dict(env),
            check=True,
        )

    @contextmanager
    def _scheduler_lock(
        self,
    ) -> Iterator[None]:
        self.lock_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        handle = self.lock_path.open(
            "a+",
            encoding="utf-8",
        )

        try:
            try:
                fcntl.flock(
                    handle.fileno(),
                    fcntl.LOCK_EX
                    | fcntl.LOCK_NB,
                )
            except BlockingIOError as exc:
                raise CurrentRefreshBusyError(
                    "Current refresh scheduler "
                    "is already running"
                ) from exc

            yield

        finally:
            try:
                fcntl.flock(
                    handle.fileno(),
                    fcntl.LOCK_UN,
                )
            finally:
                handle.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh Current Gold/Analytics "
            "and conditionally retrain ML."
        )
    )

    parser.add_argument(
        "--run-once",
        action="store_true",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
    )

    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=int(
            os.getenv(
                "CURRENT_REFRESH_INTERVAL_SECONDS",
                DEFAULT_CHECK_INTERVAL_SECONDS,
            )
        ),
    )

    parser.add_argument(
        "--min-new-rows",
        type=int,
        default=int(
            os.getenv(
                "CURRENT_REFRESH_MIN_NEW_ROWS",
                DEFAULT_MIN_NEW_ROWS,
            )
        ),
    )

    parser.add_argument(
        "--max-age-days",
        type=int,
        default=int(
            os.getenv(
                "CURRENT_REFRESH_MAX_AGE_DAYS",
                DEFAULT_MAX_AGE_DAYS,
            )
        ),
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    scheduler = CurrentRefreshScheduler(
        min_new_rows=args.min_new_rows,
        max_age_days=args.max_age_days,
    )

    if args.run_once or args.dry_run:
        return scheduler.run_once(
            dry_run=args.dry_run
        )

    if args.interval_seconds <= 0:
        raise ValueError(
            "interval-seconds must be > 0"
        )

    while True:
        try:
            scheduler.run_once()
        except Exception as exc:
            print(
                f"Current refresh failed: {exc}",
                file=sys.stderr,
            )

        time.sleep(
            args.interval_seconds
        )


if __name__ == "__main__":
    raise SystemExit(main())
