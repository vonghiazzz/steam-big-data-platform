import json
import tempfile
import unittest
from pathlib import Path

from src.common.jsonl import read_jsonl, write_jsonl
from src.discovery.policy import DEFAULT_POLICY_PATH
from src.discovery.registry import (
    STATUS_NEW,
    STATUS_QUEUED,
    RegistryEntry,
    load_registry,
    save_registry,
)
from src.discovery.scheduler import (
    load_scheduler_state,
    save_scheduler_state,
)
from src.onboarding.abandon_stale_failed_batch import (
    abandon_stale_failed_batch,
)
from src.onboarding.recover_failed_batch import (
    recover_failed_batch,
)


class FailedBatchRecoveryTest(unittest.TestCase):
    def _write_failed_batch(
        self,
        root: Path,
        batch_id: str,
        appid: int,
    ) -> None:
        batch_root = (
            root
            / "onboarding"
            / batch_id
        )
        batch_root.mkdir(
            parents=True,
            exist_ok=True,
        )

        (
            batch_root
            / "workflow_state.json"
        ).write_text(
            json.dumps(
                {
                    "batch_id": batch_id,
                    "status": "FAILED",
                    "current_phase": "crawl",
                    "steps": {
                        "runtime": {
                            "status": "COMPLETED",
                        },
                        "prepare": {
                            "status": "COMPLETED",
                        },
                        "crawl": {
                            "status": "FAILED",
                        },
                    },
                }
            ),
            encoding="utf-8",
        )

        (
            batch_root
            / "manifest.json"
        ).write_text(
            json.dumps(
                {
                    "batch_id": batch_id,
                    "games": [
                        {
                            "appid": appid,
                            "name": f"Game {appid}",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

    def test_recover_failed_crawl_releases_batch_safely(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            batch_id = "failed-batch"
            appid = 123

            self._write_failed_batch(
                root,
                batch_id,
                appid,
            )

            save_registry(
                root / "registry.jsonl",
                {
                    appid: RegistryEntry(
                        appid,
                        STATUS_QUEUED,
                        "TEST",
                        1,
                        name=f"Game {appid}",
                    )
                },
            )

            write_jsonl(
                root / "queue.jsonl",
                [
                    {
                        "appid": appid,
                        "name": f"Game {appid}",
                        "target_reviews": 500,
                    }
                ],
            )

            save_scheduler_state(
                root / "scheduler.json",
                {
                    "schema_version": 1,
                    "active_batch_id": batch_id,
                },
            )

            result = recover_failed_batch(
                batch_id,
                staging_root=(
                    root / "onboarding"
                ),
                registry_path=(
                    root / "registry.jsonl"
                ),
                queue_path=(
                    root / "queue.jsonl"
                ),
                scheduler_state_path=(
                    root / "scheduler.json"
                ),
                policy_path=DEFAULT_POLICY_PATH,
                reason="test recovery",
            )

            self.assertEqual(
                "ABANDONED",
                result["status"],
            )
            self.assertEqual(
                [appid],
                result["released_appids"],
            )

            registry = load_registry(
                root / "registry.jsonl"
            )

            self.assertEqual(
                STATUS_NEW,
                registry[appid].status,
            )

            self.assertEqual(
                [],
                read_jsonl(
                    root / "queue.jsonl"
                ),
            )

            workflow_state = json.loads(
                (
                    root
                    / "onboarding"
                    / batch_id
                    / "workflow_state.json"
                ).read_text(
                    encoding="utf-8"
                )
            )

            self.assertEqual(
                "ABANDONED",
                workflow_state["status"],
            )

            scheduler_state = (
                load_scheduler_state(
                    root / "scheduler.json"
                )
            )

            self.assertIsNone(
                scheduler_state[
                    "active_batch_id"
                ]
            )
            self.assertEqual(
                batch_id,
                scheduler_state[
                    "last_recovered_batch_id"
                ],
            )

    def test_abandoned_batch_cannot_be_recovered_again(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            batch_id = "already-abandoned"
            appid = 456

            self._write_failed_batch(
                root,
                batch_id,
                appid,
            )

            save_registry(
                root / "registry.jsonl",
                {
                    appid: RegistryEntry(
                        appid,
                        STATUS_QUEUED,
                        "TEST",
                        1,
                        name=f"Game {appid}",
                    )
                },
            )

            write_jsonl(
                root / "queue.jsonl",
                [
                    {
                        "appid": appid,
                        "name": f"Game {appid}",
                        "target_reviews": 500,
                    }
                ],
            )

            save_scheduler_state(
                root / "scheduler.json",
                {
                    "schema_version": 1,
                    "active_batch_id": batch_id,
                },
            )

            recover_failed_batch(
                batch_id,
                staging_root=(
                    root / "onboarding"
                ),
                registry_path=(
                    root / "registry.jsonl"
                ),
                queue_path=(
                    root / "queue.jsonl"
                ),
                scheduler_state_path=(
                    root / "scheduler.json"
                ),
                policy_path=DEFAULT_POLICY_PATH,
                reason="first recovery",
            )

            with self.assertRaisesRegex(
                RuntimeError,
                "FAILED workflows",
            ):
                recover_failed_batch(
                    batch_id,
                    staging_root=(
                        root / "onboarding"
                    ),
                    registry_path=(
                        root / "registry.jsonl"
                    ),
                    queue_path=(
                        root / "queue.jsonl"
                    ),
                    scheduler_state_path=(
                        root / "scheduler.json"
                    ),
                    policy_path=DEFAULT_POLICY_PATH,
                    reason="must not resurrect",
                )

    def test_abandon_metadata_only_requires_released_new_game(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            batch_id = "stale-failed"
            appid = 789

            self._write_failed_batch(
                root,
                batch_id,
                appid,
            )

            save_registry(
                root / "registry.jsonl",
                {
                    appid: RegistryEntry(
                        appid,
                        STATUS_NEW,
                        "TEST",
                        1,
                        name=f"Game {appid}",
                    )
                },
            )

            write_jsonl(
                root / "queue.jsonl",
                [],
            )

            save_scheduler_state(
                root / "scheduler.json",
                {
                    "schema_version": 1,
                    "active_batch_id": None,
                },
            )

            result = abandon_stale_failed_batch(
                batch_id,
                staging_root=(
                    root / "onboarding"
                ),
                registry_path=(
                    root / "registry.jsonl"
                ),
                queue_path=(
                    root / "queue.jsonl"
                ),
                scheduler_state_path=(
                    root / "scheduler.json"
                ),
                reason="terminalize stale metadata",
            )

            self.assertEqual(
                "ABANDONED",
                result["status"],
            )
            self.assertFalse(
                result["registry_mutated"]
            )
            self.assertFalse(
                result["queue_mutated"]
            )

            registry = load_registry(
                root / "registry.jsonl"
            )

            self.assertEqual(
                STATUS_NEW,
                registry[appid].status,
            )

            self.assertEqual(
                [],
                read_jsonl(
                    root / "queue.jsonl"
                ),
            )


if __name__ == "__main__":
    unittest.main()
