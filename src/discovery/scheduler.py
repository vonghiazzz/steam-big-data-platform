"""Small WEEKLY scheduler for bounded discovery and resumable onboarding."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator, Mapping

from src.common.jsonl import read_jsonl
from src.discovery.paths import (
    DISCOVERY_SCHEDULER_LOCK_PATH,
    DISCOVERY_SCHEDULER_STATE_PATH,
    ONBOARDING_QUEUE_PATH,
    PROJECT_ROOT,
    REGISTRY_PATH,
)
from src.discovery.policy import DEFAULT_POLICY_PATH, load_discovery_policy
from src.discovery.registry import (
    OnboardingCapacity,
    calculate_onboarding_capacity,
    load_registry,
)


DEFAULT_STAGING_ROOT = PROJECT_ROOT / "data" / "onboarding"
DEFAULT_CHECK_INTERVAL_SECONDS = 3600
COMPLETED = "COMPLETED"
CommandRunner = Callable[[list[str]], object]


class SchedulerBusyError(RuntimeError):
    """Raised when another scheduler process owns the scheduler lock."""


@dataclass(frozen=True)
class SchedulerPlan:
    frequency: str
    period: str
    due: bool
    action: str
    capacity: OnboardingCapacity
    queue_count: int
    unfinished_batch_id: str | None
    required_onboarding_slots: int

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["capacity"] = asdict(self.capacity)
        return payload


def weekly_period(now: datetime) -> str:
    iso = now.astimezone(timezone.utc).isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def weekly_due(state: Mapping[str, object], period: str) -> bool:
    return state.get("last_successful_period") != period


def load_scheduler_state(path: Path) -> dict:
    if not path.exists():
        return {
            "schema_version": 1,
            "last_attempt_at": None,
            "last_attempt_period": None,
            "last_attempt_status": None,
            "last_error": None,
            "last_successful_run": None,
            "last_successful_period": None,
            "active_batch_id": None,
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid scheduler state JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Scheduler state must be an object: {path}")
    return payload


def save_scheduler_state(path: Path, state: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{os.getpid()}.tmp"
    temporary.write_text(
        json.dumps(state, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def find_unfinished_batch(staging_root: Path) -> tuple[str | None, int]:
    unfinished: list[tuple[str, int]] = []
    if not staging_root.exists():
        return None, 0
    for state_path in sorted(staging_root.glob("*/workflow_state.json")):
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        if payload.get("status") == COMPLETED:
            continue
        batch_id = str(payload.get("batch_id") or state_path.parent.name)
        manifest_path = state_path.parent / "manifest.json"
        game_count = 0
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            games = manifest.get("games")
            if isinstance(games, list):
                game_count = len(games)
        unfinished.append((batch_id, game_count))
    if len(unfinished) > 1:
        raise RuntimeError(
            "Multiple unfinished onboarding batches require operator review: "
            + ", ".join(batch_id for batch_id, _count in unfinished)
        )
    return unfinished[0] if unfinished else (None, 0)


class DiscoveryScheduler:
    def __init__(
        self,
        *,
        policy_path: Path = DEFAULT_POLICY_PATH,
        registry_path: Path = REGISTRY_PATH,
        queue_path: Path = ONBOARDING_QUEUE_PATH,
        state_path: Path = DISCOVERY_SCHEDULER_STATE_PATH,
        lock_path: Path = DISCOVERY_SCHEDULER_LOCK_PATH,
        staging_root: Path = DEFAULT_STAGING_ROOT,
        max_active_games: int = 100,
        command_runner: CommandRunner | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.policy_path = policy_path
        self.registry_path = registry_path
        self.queue_path = queue_path
        self.state_path = state_path
        self.lock_path = lock_path
        self.staging_root = staging_root
        self.max_active_games = max_active_games
        self.command_runner = command_runner or self._run_command
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def plan(self, *, force: bool = False) -> SchedulerPlan:
        policy = load_discovery_policy(self.policy_path)
        frequency = policy.discovery.frequency.upper()
        if frequency != "WEEKLY":
            raise ValueError(f"Unsupported discovery frequency: {frequency}")
        registry = load_registry(self.registry_path)
        capacity = calculate_onboarding_capacity(
            registry,
            max_active_games=self.max_active_games,
            max_new_games_per_cycle=policy.onboarding.max_new_games_per_cycle,
        )
        state = load_scheduler_state(self.state_path)
        period = weekly_period(self.clock())
        due = force or weekly_due(state, period)
        unfinished_batch_id, manifest_slots = find_unfinished_batch(
            self.staging_root
        )
        active_batch_id = state.get("active_batch_id")
        if active_batch_id and unfinished_batch_id not in (None, active_batch_id):
            raise RuntimeError(
                "Scheduler active batch conflicts with unfinished workflow: "
                f"{active_batch_id} != {unfinished_batch_id}"
            )
        if active_batch_id and unfinished_batch_id is None:
            active_state = (
                self.staging_root / str(active_batch_id) / "workflow_state.json"
            )
            if not active_state.exists():
                # The workflow may have failed before its first state write.
                # Retrying the same batch ID is safe and avoids replacement.
                unfinished_batch_id = str(active_batch_id)
                manifest_slots = 0
            else:
                payload = json.loads(active_state.read_text(encoding="utf-8"))
            if active_state.exists() and payload.get("status") == COMPLETED:
                unfinished_batch_id = str(active_batch_id)
                manifest_slots = -1

        queue_count = len(read_jsonl(self.queue_path)) if self.queue_path.exists() else 0
        required_slots = manifest_slots or queue_count
        if unfinished_batch_id and manifest_slots == -1:
            action = "FINALIZE_COMPLETED_BATCH"
        elif unfinished_batch_id:
            action = (
                "RESUME_ONBOARDING"
                if required_slots <= capacity.cycle_capacity
                else "WAIT_FOR_ONBOARDING_CAPACITY"
            )
        elif not due:
            action = "SKIP_NOT_DUE"
        else:
            action = "REFRESH_DISCOVER"
        return SchedulerPlan(
            frequency=frequency,
            period=period,
            due=due,
            action=action,
            capacity=capacity,
            queue_count=queue_count,
            unfinished_batch_id=unfinished_batch_id,
            required_onboarding_slots=max(0, required_slots),
        )

    def run_once(self, *, dry_run: bool = False, force: bool = False) -> int:
        with self._scheduler_lock():
            plan = self.plan(force=force)
            print(json.dumps(plan.to_dict(), indent=2, ensure_ascii=False))
            if dry_run or plan.action == "SKIP_NOT_DUE":
                return 0

            state = load_scheduler_state(self.state_path)
            attempted_at = self.clock().astimezone(timezone.utc).isoformat()
            state.update(
                {
                    "last_attempt_at": attempted_at,
                    "last_attempt_period": plan.period,
                    "last_attempt_status": "RUNNING",
                    "last_error": None,
                }
            )
            save_scheduler_state(self.state_path, state)
            try:
                if plan.action == "FINALIZE_COMPLETED_BATCH":
                    state["active_batch_id"] = None
                    self._record_success(state, plan.period)
                    return 0
                if plan.action == "WAIT_FOR_ONBOARDING_CAPACITY":
                    state["last_attempt_status"] = "CAPACITY_SKIP"
                    self._record_success(state, plan.period)
                    return 0
                if plan.action == "RESUME_ONBOARDING":
                    batch_id = str(plan.unfinished_batch_id)
                    state["active_batch_id"] = batch_id
                    save_scheduler_state(self.state_path, state)
                    self._invoke(self._onboarding_command(batch_id))
                    state["active_batch_id"] = None
                    self._record_success(state, plan.period)
                    return 0

                self._invoke(self._discovery_command())
                post_registry = load_registry(self.registry_path)
                policy = load_discovery_policy(self.policy_path)
                post_capacity = calculate_onboarding_capacity(
                    post_registry,
                    max_active_games=self.max_active_games,
                    max_new_games_per_cycle=(
                        policy.onboarding.max_new_games_per_cycle
                    ),
                )
                queue_count = (
                    len(read_jsonl(self.queue_path))
                    if self.queue_path.exists()
                    else 0
                )
                if queue_count == 0 or queue_count > post_capacity.cycle_capacity:
                    state["last_attempt_status"] = (
                        "COMPLETED_NO_QUEUE"
                        if queue_count == 0
                        else "COMPLETED_CAPACITY_SKIP"
                    )
                    self._record_success(state, plan.period)
                    return 0

                batch_id = self._new_batch_id(plan.period)
                state["active_batch_id"] = batch_id
                save_scheduler_state(self.state_path, state)
                self._invoke(self._onboarding_command(batch_id))
                state["active_batch_id"] = None
                self._record_success(state, plan.period)
                return 0
            except Exception as exc:
                state["last_attempt_status"] = "FAILED"
                state["last_error"] = str(exc)
                save_scheduler_state(self.state_path, state)
                print(f"scheduler_error={exc}", file=sys.stderr)
                return 1

    def _record_success(self, state: dict, period: str) -> None:
        completed_at = self.clock().astimezone(timezone.utc).isoformat()
        if state.get("last_attempt_status") == "RUNNING":
            state["last_attempt_status"] = "COMPLETED"
        state["last_error"] = None
        state["last_successful_run"] = completed_at
        state["last_successful_period"] = period
        save_scheduler_state(self.state_path, state)

    def _discovery_command(self) -> list[str]:
        return [
            sys.executable,
            "-m",
            "src.discovery.run_discovery",
            "--policy-path",
            str(self.policy_path),
            "--registry-path",
            str(self.registry_path),
            "--queue-path",
            str(self.queue_path),
            "--refresh-catalog",
            "--max-active-games",
            str(self.max_active_games),
        ]

    def _onboarding_command(self, batch_id: str) -> list[str]:
        return [
            sys.executable,
            "-m",
            "src.onboarding.run_workflow",
            "--batch-id",
            batch_id,
            "--queue",
            str(self.queue_path),
            "--registry",
            str(self.registry_path),
            "--policy",
            str(self.policy_path),
            "--staging-root",
            str(self.staging_root),
        ]

    def _new_batch_id(self, period: str) -> str:
        stamp = self.clock().astimezone(timezone.utc).strftime("%Y%m%dt%H%M%Sz")
        return f"scheduled-{period.lower()}-{stamp}"

    def _invoke(self, command: list[str]) -> None:
        result = self.command_runner(command)
        returncode = result if isinstance(result, int) else getattr(result, "returncode", 1)
        if returncode != 0:
            raise RuntimeError(
                f"command failed with exit code {returncode}: {' '.join(command)}"
            )

    @staticmethod
    def _run_command(command: list[str]) -> subprocess.CompletedProcess:
        environment = dict(os.environ)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        return subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            env=environment,
            check=False,
        )

    @contextmanager
    def _scheduler_lock(self) -> Iterator[None]:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+", encoding="utf-8") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise SchedulerBusyError(
                    "Another discovery scheduler is already running"
                ) from exc
            yield


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY_PATH)
    parser.add_argument("--registry", type=Path, default=REGISTRY_PATH)
    parser.add_argument("--queue", type=Path, default=ONBOARDING_QUEUE_PATH)
    parser.add_argument("--state", type=Path, default=DISCOVERY_SCHEDULER_STATE_PATH)
    parser.add_argument("--lock", type=Path, default=DISCOVERY_SCHEDULER_LOCK_PATH)
    parser.add_argument("--staging-root", type=Path, default=DEFAULT_STAGING_ROOT)
    parser.add_argument(
        "--max-active-games",
        type=int,
        default=int(os.getenv("STREAM_MAX_ACTIVE_GAMES", "100")),
    )
    parser.add_argument(
        "--check-interval-seconds",
        type=int,
        default=int(
            os.getenv(
                "DISCOVERY_SCHEDULER_CHECK_INTERVAL_SECONDS",
                str(DEFAULT_CHECK_INTERVAL_SECONDS),
            )
        ),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.max_active_games <= 0:
        raise SystemExit("--max-active-games must be positive")
    if args.check_interval_seconds <= 0:
        raise SystemExit("--check-interval-seconds must be positive")
    if args.dry_run and not args.run_once:
        raise SystemExit("--dry-run requires --run-once")
    scheduler = DiscoveryScheduler(
        policy_path=args.policy,
        registry_path=args.registry,
        queue_path=args.queue,
        state_path=args.state,
        lock_path=args.lock,
        staging_root=args.staging_root,
        max_active_games=args.max_active_games,
    )
    if args.run_once:
        try:
            return scheduler.run_once(dry_run=args.dry_run, force=args.force)
        except SchedulerBusyError as exc:
            print(f"scheduler_busy={exc}", file=sys.stderr)
            return 2
    while True:
        try:
            scheduler.run_once()
        except SchedulerBusyError as exc:
            print(f"scheduler_busy={exc}", file=sys.stderr)
        time.sleep(args.check_interval_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
