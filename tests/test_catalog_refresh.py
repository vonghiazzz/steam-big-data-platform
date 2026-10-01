import tempfile
import unittest
from dataclasses import replace
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch

from src.common.jsonl import read_jsonl, write_jsonl
from src.discovery.catalog_probe import probe_catalog
from src.discovery.policy import load_discovery_policy
from src.discovery.refresh_catalog import refresh_catalog
from src.discovery.registry import (
    STATUS_ACTIVE,
    STATUS_NEW,
    STATUS_QUEUED,
    STATUS_RETIRED,
    RegistryEntry,
    reconcile_onboarding_queue,
)
from src.discovery.run_discovery import run_control_plane


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


def staged_probe(output_dir: Path, page_size: int, max_pages: int, delay: float):
    del page_size, max_pages, delay
    write_jsonl(
        output_dir / "candidates.jsonl",
        [{"appid": 10, "title": "Ten"}, {"appid": 20, "title": "Twenty"}],
    )


def staged_metadata(input_path: Path, output_dir: Path, delay: float):
    del input_path, delay
    write_jsonl(
        output_dir / "metadata_probe_raw.jsonl",
        [
            {
                "appid": appid,
                "success": True,
                "data": {
                    "name": name,
                    "type": "game",
                    "release_date": {"date": "1 Jan, 2020"},
                },
            }
            for appid, name in ((10, "Ten"), (20, "Twenty"))
        ],
    )
    write_jsonl(
        output_dir / "eligible_games.jsonl",
        [
            {"appid": 10, "name": "Ten", "type": "game"},
            {"appid": 20, "name": "Twenty", "type": "game"},
        ],
    )


def staged_reviews(
    input_path: Path,
    output_path: Path,
    delay: float,
    policy_path: Path,
):
    del input_path, delay, policy_path
    write_jsonl(
        output_path,
        [
            {"appid": 10, "query_summary": {"total_reviews": 2000}},
            {"appid": 20, "query_summary": {"total_reviews": 3000}},
        ],
    )


class CatalogRefreshTest(unittest.TestCase):
    def setUp(self):
        self.policy = load_discovery_policy()

    def test_catalog_probe_extracts_deduplicates_and_stops_at_page_bound(self):
        html = """
        <a class="search_result_row" data-ds-appid="10">
          <span class="title">Ten</span>
          <div class="search_released">1 Jan, 2020</div>
        </a>
        <a class="search_result_row" data-ds-appid="20">
          <span class="title">Twenty</span>
          <div class="search_released">2 Jan, 2020</div>
        </a>
        """
        responses = []
        for _ in range(2):
            response = Mock(status_code=200)
            response.json.return_value = {
                "success": 1,
                "total_count": 269151,
                "results_html": html,
            }
            responses.append(response)

        with tempfile.TemporaryDirectory() as directory:
            with patch(
                "src.discovery.catalog_probe.requests.get", side_effect=responses
            ) as request:
                rows = probe_catalog(Path(directory), 2, 2, 0)

            self.assertEqual(2, request.call_count)
            self.assertEqual([10, 20], [row["appid"] for row in rows])
            self.assertEqual(
                [10, 20],
                [row["appid"] for row in read_jsonl(Path(directory) / "candidates.jsonl")],
            )
            self.assertEqual(
                [0, 2],
                [call.kwargs["params"]["start"] for call in request.call_args_list],
            )

    def test_configured_bound_rejects_unbounded_override(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "exceeds configured bound"):
                refresh_catalog(
                    policy=self.policy,
                    policy_path=root / "policy.json",
                    registry_path=root / "registry.jsonl",
                    candidates_path=root / "candidates.jsonl",
                    metadata_path=root / "metadata.jsonl",
                    eligible_path=root / "eligible.jsonl",
                    review_probe_path=root / "reviews.jsonl",
                    max_pages=self.policy.catalog_refresh.max_pages + 1,
                )

    def test_failed_metadata_or_review_probe_keeps_previous_snapshot(self):
        for failed_stage in ("metadata", "review"):
            with self.subTest(failed_stage=failed_stage):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    paths = self._snapshot_paths(root)
                    previous = self._write_previous_snapshot(paths)

                    metadata_effect = (
                        RuntimeError("metadata failed")
                        if failed_stage == "metadata"
                        else staged_metadata
                    )
                    review_effect = (
                        RuntimeError("review failed")
                        if failed_stage == "review"
                        else staged_reviews
                    )
                    with patch(
                        "src.discovery.refresh_catalog.probe_catalog",
                        side_effect=staged_probe,
                    ), patch(
                        "src.discovery.refresh_catalog.qualify_catalog",
                        side_effect=metadata_effect,
                    ), patch(
                        "src.discovery.refresh_catalog.qualify_reviews",
                        side_effect=review_effect,
                    ):
                        with self.assertRaises(RuntimeError):
                            self._refresh(paths)

                    self.assertEqual(
                        previous,
                        {name: path.read_bytes() for name, path in paths.items()},
                    )

    def test_successful_refresh_promotes_complete_staged_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = self._snapshot_paths(root)
            self._write_previous_snapshot(paths)
            with patch(
                "src.discovery.refresh_catalog.probe_catalog",
                side_effect=staged_probe,
            ), patch(
                "src.discovery.refresh_catalog.qualify_catalog",
                side_effect=staged_metadata,
            ), patch(
                "src.discovery.refresh_catalog.qualify_reviews",
                side_effect=staged_reviews,
            ):
                result = self._refresh(paths)

            self.assertEqual(2, result.candidate_count)
            self.assertEqual(2, result.metadata_count)
            self.assertEqual(2, result.review_probe_count)
            self.assertEqual((10, 20), result.unseen_appids)
            self.assertEqual(
                [10, 20], [row["appid"] for row in read_jsonl(paths["candidates"])]
            )

    def test_lifecycle_and_queue_reconciliation_preserve_existing_state(self):
        policy = replace(
            self.policy,
            onboarding=replace(
                self.policy.onboarding,
                max_new_games_per_cycle=2,
            ),
        )
        registry = {
            1: RegistryEntry(1, STATUS_ACTIVE, "TEST", 1, name="Active"),
            2: RegistryEntry(2, STATUS_RETIRED, "TEST", 1, name="Retired"),
            3: RegistryEntry(3, STATUS_QUEUED, "TEST", 1, name="Queued"),
        }
        result = run_control_plane(
            [
                valid_candidate(1),
                valid_candidate(2),
                valid_candidate(30),
                valid_candidate(31),
            ],
            [],
            registry,
            policy,
            current_date=AS_OF_DATE,
            run_timestamp="2026-10-01T00:00:00+00:00",
        )
        queue = reconcile_onboarding_queue(
            [
                {"appid": 3, "name": "Queued", "target_reviews": 500},
                {"appid": 3, "name": "Duplicate", "target_reviews": 500},
            ],
            result.registry,
            policy.onboarding,
        )

        self.assertEqual(STATUS_ACTIVE, result.registry[1].status)
        self.assertEqual(STATUS_RETIRED, result.registry[2].status)
        self.assertEqual(STATUS_QUEUED, result.registry[3].status)
        self.assertEqual(STATUS_QUEUED, result.registry[30].status)
        self.assertEqual(STATUS_NEW, result.registry[31].status)
        self.assertEqual(1, result.onboarding_plan.queued_count)
        self.assertEqual([3, 30], [row["appid"] for row in queue])
        self.assertEqual(len(queue), len({row["appid"] for row in queue}))

    def _refresh(self, paths: dict[str, Path]):
        return refresh_catalog(
            policy=self.policy,
            policy_path=Path("config/discovery_policy.json"),
            registry_path=paths["registry"],
            candidates_path=paths["candidates"],
            metadata_path=paths["metadata"],
            eligible_path=paths["eligible"],
            review_probe_path=paths["reviews"],
            max_pages=1,
            delay=0,
        )

    @staticmethod
    def _snapshot_paths(root: Path) -> dict[str, Path]:
        return {
            "candidates": root / "candidates.jsonl",
            "metadata": root / "metadata_probe_raw.jsonl",
            "eligible": root / "eligible_games.jsonl",
            "reviews": root / "review_probe.jsonl",
            "registry": root / "registry.jsonl",
        }

    @staticmethod
    def _write_previous_snapshot(paths: dict[str, Path]) -> dict[str, bytes]:
        write_jsonl(paths["candidates"], [{"appid": 99}])
        write_jsonl(paths["metadata"], [{"appid": 99}])
        write_jsonl(paths["eligible"], [{"appid": 99}])
        write_jsonl(paths["reviews"], [{"appid": 99}])
        write_jsonl(
            paths["registry"],
            [
                {
                    "appid": 99,
                    "status": "ACTIVE",
                    "source": "TEST",
                    "policy_version": 1,
                    "name": "Previous",
                }
            ],
        )
        return {name: path.read_bytes() for name, path in paths.items()}


if __name__ == "__main__":
    unittest.main()
