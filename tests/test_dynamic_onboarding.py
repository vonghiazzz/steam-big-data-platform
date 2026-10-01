import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

from src.common.jsonl import write_jsonl
from src.discovery.policy import load_discovery_policy
from src.discovery.registry import (
    STATUS_ACTIVE,
    STATUS_QUEUED,
    RegistryEntry,
)
from src.onboarding.activation import GameReadiness, build_activated_registry
from src.onboarding.manifest import build_manifest, load_manifest
from src.onboarding.publisher import _publish_one
from src.onboarding.staging import prepare_batch_layout
from src.onboarding.validation import validate_staged_batch


def review_row(appid: int, recommendationid: str) -> dict:
    return {
        "appid": appid,
        "game_name": f"Game {appid}",
        "review": {
            "recommendationid": recommendationid,
            "language": "english",
            "voted_up": True,
            "timestamp_created": 1_700_000_000,
        },
    }


class DynamicOnboardingTest(unittest.TestCase):
    def setUp(self):
        policy = load_discovery_policy()
        self.policy = replace(
            policy,
            onboarding=replace(
                policy.onboarding,
                target_reviews_per_game=2,
                max_new_games_per_cycle=2,
            ),
        )
        self.registry = {
            10: RegistryEntry(10, STATUS_QUEUED, "TEST", 1, name="Ten"),
            20: RegistryEntry(20, STATUS_QUEUED, "TEST", 1, name="Twenty"),
        }
        self.queue = [
            {"appid": 10, "name": "Ten", "target_reviews": 2},
            {"appid": 20, "name": "Twenty", "target_reviews": 2},
        ]

    def manifest(self):
        return build_manifest(
            "batch-001",
            self.queue,
            self.registry,
            self.policy,
            created_at="2026-09-30T00:00:00+00:00",
        )

    def test_manifest_requires_queued_registry_entries(self):
        registry = dict(self.registry)
        registry[10] = replace(registry[10], status=STATUS_ACTIVE)
        with self.assertRaisesRegex(ValueError, "expected QUEUED"):
            build_manifest("batch-001", self.queue, registry, self.policy)

    def test_manifest_rejects_duplicate_queue_appid(self):
        with self.assertRaisesRegex(ValueError, "Duplicate queue appid"):
            build_manifest(
                "batch-001",
                [self.queue[0], self.queue[0]],
                self.registry,
                self.policy,
            )

    def test_prepare_stages_only_manifest_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            probe = root / "metadata.jsonl"
            write_jsonl(
                probe,
                [
                    {"appid": 10, "success": True, "data": {"name": "Ten"}},
                    {"appid": 20, "success": True, "data": {"name": "Twenty"}},
                    {"appid": 30, "success": True, "data": {"name": "Thirty"}},
                ],
            )
            batch = prepare_batch_layout(root / "staging", self.manifest(), probe)
            loaded = load_manifest(batch / "manifest.json")
            metadata = [
                json.loads(line)
                for line in (batch / "games/games_raw.jsonl").read_text().splitlines()
            ]
            self.assertEqual((10, 20), loaded.appids)
            self.assertEqual([10, 20], [row["appid"] for row in metadata])

    def test_dynamic_validation_passes_without_hard_coded_counts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_valid_stage(root)
            baseline = root / "baseline"
            baseline.mkdir()
            write_jsonl(baseline / "1.jsonl", [review_row(1, "old")])
            report = validate_staged_batch(
                self.manifest(), root, baseline_reviews_root=baseline
            )
            self.assertTrue(report.passed, report.errors)
            self.assertEqual(4, report.total_reviews)
            self.assertEqual({10: 2, 20: 2}, report.per_game_counts)

    def test_dynamic_validation_rejects_baseline_overlap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_valid_stage(root)
            baseline = root / "baseline"
            baseline.mkdir()
            write_jsonl(baseline / "1.jsonl", [review_row(1, "ten-1")])
            report = validate_staged_batch(
                self.manifest(), root, baseline_reviews_root=baseline
            )
            self.assertFalse(report.passed)
            self.assertEqual(1, report.baseline_overlap_count)

    def test_activation_requires_every_materialization_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_valid_stage(root)
            report = validate_staged_batch(self.manifest(), root)
            readiness = {
                appid: GameReadiness(
                    appid=appid,
                    bronze_metadata=True,
                    bronze_reviews=2,
                    silver_game=True,
                    silver_reviews=2,
                    gold_reviews=2,
                    mongo_game_metrics=True,
                )
                for appid in (10, 20)
            }
            readiness[20] = replace(readiness[20], gold_reviews=1)
            with self.assertRaisesRegex(ValueError, "gold_reviews"):
                build_activated_registry(
                    self.registry,
                    self.manifest(),
                    report,
                    readiness,
                    activated_at="2026-09-30T01:00:00+00:00",
                )

    def test_activation_returns_active_copy_after_all_gates_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_valid_stage(root)
            report = validate_staged_batch(self.manifest(), root)
            readiness = {
                appid: GameReadiness(
                    appid=appid,
                    bronze_metadata=True,
                    bronze_reviews=2,
                    silver_game=True,
                    silver_reviews=2,
                    gold_reviews=2,
                    mongo_game_metrics=True,
                )
                for appid in (10, 20)
            }
            activated = build_activated_registry(
                self.registry,
                self.manifest(),
                report,
                readiness,
                activated_at="2026-09-30T01:00:00+00:00",
            )
            self.assertEqual(STATUS_ACTIVE, activated[10].status)
            self.assertEqual(STATUS_ACTIVE, activated[20].status)
            self.assertEqual(STATUS_QUEUED, self.registry[10].status)

    def test_publish_resume_reuses_identical_hdfs_object(self):
        with tempfile.TemporaryDirectory() as directory:
            local = Path(directory) / "10.jsonl"
            local.write_bytes(b'{"appid": 10}\n')
            responses = [
                CompletedProcess([], 0, "", ""),
                CompletedProcess([], 0, local.read_bytes(), b""),
            ]
            uploaded: list[str] = []
            reused: list[str] = []
            with patch("src.onboarding.publisher._hdfs", side_effect=responses):
                _publish_one(
                    local,
                    "/tmp/10.jsonl",
                    "/steam/bronze/reviews/10.jsonl",
                    "namenode",
                    uploaded,
                    reused,
                )
            self.assertEqual([], uploaded)
            self.assertEqual(["/steam/bronze/reviews/10.jsonl"], reused)

    def test_publish_resume_rejects_non_identical_hdfs_object(self):
        with tempfile.TemporaryDirectory() as directory:
            local = Path(directory) / "10.jsonl"
            local.write_bytes(b'{"appid": 10}\n')
            responses = [
                CompletedProcess([], 0, "", ""),
                CompletedProcess([], 0, b'{"appid": 99}\n', b""),
            ]
            with patch("src.onboarding.publisher._hdfs", side_effect=responses):
                with self.assertRaisesRegex(RuntimeError, "Refusing to overwrite"):
                    _publish_one(
                        local,
                        "/tmp/10.jsonl",
                        "/steam/bronze/reviews/10.jsonl",
                        "namenode",
                        [],
                        [],
                    )

    @staticmethod
    def _write_valid_stage(root: Path) -> None:
        write_jsonl(
            root / "games/games_raw.jsonl",
            [
                {"appid": 10, "success": True, "data": {"name": "Ten"}},
                {"appid": 20, "success": True, "data": {"name": "Twenty"}},
            ],
        )
        write_jsonl(
            root / "reviews_by_game/10.jsonl",
            [review_row(10, "ten-1"), review_row(10, "ten-2")],
        )
        write_jsonl(
            root / "reviews_by_game/20.jsonl",
            [review_row(20, "twenty-1"), review_row(20, "twenty-2")],
        )


if __name__ == "__main__":
    unittest.main()
