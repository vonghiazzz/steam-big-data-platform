import fcntl
import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from src.common.jsonl import write_jsonl
from src.discovery.policy import DEFAULT_POLICY_PATH, load_discovery_policy
from src.discovery.registry import (
    STATUS_ACTIVE,
    STATUS_NEW,
    STATUS_QUEUED,
    RegistryEntry,
    calculate_onboarding_capacity,
    reconcile_onboarding_queue,
    save_registry,
)
from src.discovery.run_discovery import run_control_plane
from src.discovery.scheduler import (
    DiscoveryScheduler,
    SchedulerBusyError,
    find_unfinished_batch,
    load_scheduler_state,
    save_scheduler_state,
    weekly_period,
)


NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
AS_OF_DATE = date(2026, 10, 1)


def valid_candidate(appid: int) -> dict:
    return {
        "appid": appid,
        "name": f"Game {appid}",
        "app_type": "game",
        "release_date": "2025-01-01",
        "total_reviews": 2000,
        "metadata_available": True,
        "review_endpoint_available": True,
    }


def registry_with_counts(active: int, queued: int = 0, new: int = 0):
    registry = {}
    appid = 1
    for status, count in (
        (STATUS_ACTIVE, active),
        (STATUS_QUEUED, queued),
        (STATUS_NEW, new),
    ):
        for _ in range(count):
            registry[appid] = RegistryEntry(
                appid,
                status,
                "TEST",
                1,
                name=f"Game {appid}",
            )
            appid += 1
    return registry


class DiscoverySchedulerTest(unittest.TestCase):
    def setUp(self):
        self.policy = load_discovery_policy()

    def test_capacity_at_90_95_and_100_active(self):
        expected = ((90, 10), (95, 5), (100, 0))
        for active_count, expected_cycle_capacity in expected:
            with self.subTest(active_count=active_count):
                capacity = calculate_onboarding_capacity(
                    registry_with_counts(active_count),
                    max_active_games=100,
                    max_new_games_per_cycle=10,
                )
                self.assertEqual(expected_cycle_capacity, capacity.cycle_capacity)
                self.assertEqual(
                    expected_cycle_capacity,
                    capacity.new_queue_capacity,
                )

    def test_existing_queued_games_reserve_capacity(self):
        capacity = calculate_onboarding_capacity(
            registry_with_counts(95, queued=3),
            max_active_games=100,
            max_new_games_per_cycle=10,
        )
        self.assertEqual(5, capacity.cycle_capacity)
        self.assertEqual(2, capacity.new_queue_capacity)

    def test_zero_capacity_preserves_new_games_without_queueing(self):
        registry = registry_with_counts(100)
        result = run_control_plane(
            [valid_candidate(1001)],
            [],
            registry,
            self.policy,
            current_date=AS_OF_DATE,
            max_active_games=100,
        )
        self.assertEqual(0, result.onboarding_plan.cycle_capacity)
        self.assertEqual(0, result.onboarding_plan.queued_count)
        self.assertEqual(STATUS_NEW, result.registry[1001].status)

    def test_existing_queue_is_preserved_while_only_remaining_slots_fill(self):
        registry = registry_with_counts(95, queued=3)
        existing_queued = [
            entry.appid
            for entry in registry.values()
            if entry.status == STATUS_QUEUED
        ]
        candidates = [valid_candidate(appid) for appid in range(1001, 1006)]
        result = run_control_plane(
            candidates,
            [],
            registry,
            self.policy,
            current_date=AS_OF_DATE,
            max_active_games=100,
        )
        queue = reconcile_onboarding_queue(
            [
                {
                    "appid": appid,
                    "name": registry[appid].name,
                    "target_reviews": 500,
                }
                for appid in existing_queued
            ],
            result.registry,
            self.policy.onboarding,
        )
        self.assertEqual(2, result.onboarding_plan.queued_count)
        self.assertEqual(5, len(queue))
        self.assertTrue(set(existing_queued).issubset(row["appid"] for row in queue))

    def test_due_scheduler_runs_one_discovery_cycle_and_records_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = []
            scheduler = self._scheduler(root, 100, commands, runner_result=0)

            result = scheduler.run_once()

            self.assertEqual(0, result)
            self.assertEqual(1, len(commands))
            self.assertIn("src.discovery.run_discovery", commands[0])
            state = load_scheduler_state(root / "state.json")
            self.assertEqual("COMPLETED_NO_QUEUE", state["last_attempt_status"])
            self.assertEqual(weekly_period(NOW), state["last_successful_period"])

    def test_not_due_scheduler_skips_without_command(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = []
            scheduler = self._scheduler(root, 100, commands, runner_result=0)
            save_scheduler_state(
                root / "state.json",
                {
                    "schema_version": 1,
                    "last_successful_period": weekly_period(NOW),
                    "active_batch_id": None,
                },
            )

            result = scheduler.run_once()

            self.assertEqual(0, result)
            self.assertEqual([], commands)

    def test_failed_discovery_records_attempt_without_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = []
            scheduler = self._scheduler(root, 100, commands, runner_result=1)

            result = scheduler.run_once()

            self.assertEqual(1, result)
            state = load_scheduler_state(root / "state.json")
            self.assertEqual("FAILED", state["last_attempt_status"])
            self.assertIsNone(state.get("last_successful_period"))
            self.assertIn("command failed", state["last_error"])

    def test_overlapping_scheduler_run_is_prevented(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scheduler = self._scheduler(root, 100, [], runner_result=0)
            lock_path = root / "scheduler.lock"
            lock_path.touch()
            with lock_path.open("a+", encoding="utf-8") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaises(SchedulerBusyError):
                    scheduler.run_once(dry_run=True)

    def test_abandoned_batch_is_not_unfinished(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            batch_root = (
                root
                / "onboarding"
                / "abandoned-batch"
            )
            batch_root.mkdir(parents=True)

            (
                batch_root
                / "workflow_state.json"
            ).write_text(
                json.dumps(
                    {
                        "batch_id": "abandoned-batch",
                        "status": "ABANDONED",
                    }
                ),
                encoding="utf-8",
            )

            batch_id, slots = (
                find_unfinished_batch(
                    root / "onboarding"
                )
            )

            self.assertIsNone(batch_id)
            self.assertEqual(0, slots)

    def test_keyboard_interrupt_records_interrupted_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            save_registry(
                root / "registry.jsonl",
                registry_with_counts(100),
            )
            write_jsonl(
                root / "queue.jsonl",
                [],
            )

            def interrupted_runner(command):
                raise KeyboardInterrupt()

            scheduler = DiscoveryScheduler(
                policy_path=DEFAULT_POLICY_PATH,
                registry_path=(
                    root / "registry.jsonl"
                ),
                queue_path=(
                    root / "queue.jsonl"
                ),
                state_path=(
                    root / "state.json"
                ),
                lock_path=(
                    root / "scheduler.lock"
                ),
                staging_root=(
                    root / "onboarding"
                ),
                max_active_games=100,
                command_runner=interrupted_runner,
                clock=lambda: NOW,
            )

            with self.assertRaises(
                KeyboardInterrupt
            ):
                scheduler.run_once()

            state = load_scheduler_state(
                root / "state.json"
            )

            self.assertEqual(
                "INTERRUPTED",
                state["last_attempt_status"],
            )
            self.assertEqual(
                "Scheduler interrupted by operator",
                state["last_error"],
            )
            self.assertIsNone(
                state.get(
                    "last_successful_period"
                )
            )

    def test_unfinished_onboarding_resumes_without_new_discovery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = registry_with_counts(90, queued=1)
            save_registry(root / "registry.jsonl", registry)
            queued_appid = next(
                entry.appid
                for entry in registry.values()
                if entry.status == STATUS_QUEUED
            )
            write_jsonl(
                root / "queue.jsonl",
                [{"appid": queued_appid, "name": f"Game {queued_appid}"}],
            )
            batch_root = root / "onboarding" / "scheduled-existing"
            batch_root.mkdir(parents=True)
            (batch_root / "workflow_state.json").write_text(
                json.dumps(
                    {"batch_id": "scheduled-existing", "status": "FAILED"}
                ),
                encoding="utf-8",
            )
            (batch_root / "manifest.json").write_text(
                json.dumps({"games": [{"appid": queued_appid}]}),
                encoding="utf-8",
            )
            commands = []
            scheduler = self._scheduler(
                root,
                90,
                commands,
                runner_result=0,
                write_registry_file=False,
            )

            result = scheduler.run_once()

            self.assertEqual(0, result)
            self.assertEqual(1, len(commands))
            self.assertIn("src.onboarding.run_workflow", commands[0])
            self.assertIn("scheduled-existing", commands[0])
            self.assertNotIn("src.discovery.run_discovery", commands[0])

    def _scheduler(
        self,
        root: Path,
        active_count: int,
        commands: list,
        *,
        runner_result: int,
        write_registry_file: bool = True,
    ) -> DiscoveryScheduler:
        registry_path = root / "registry.jsonl"
        queue_path = root / "queue.jsonl"
        if write_registry_file:
            save_registry(registry_path, registry_with_counts(active_count))
            write_jsonl(queue_path, [])

        def runner(command):
            commands.append(command)
            return runner_result

        return DiscoveryScheduler(
            policy_path=DEFAULT_POLICY_PATH,
            registry_path=registry_path,
            queue_path=queue_path,
            state_path=root / "state.json",
            lock_path=root / "scheduler.lock",
            staging_root=root / "onboarding",
            max_active_games=100,
            command_runner=runner,
            clock=lambda: NOW,
        )


if __name__ == "__main__":
    unittest.main()
