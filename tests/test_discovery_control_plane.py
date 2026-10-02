import unittest
from dataclasses import replace
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

from src.common.jsonl import read_jsonl
from src.discovery.policy import load_discovery_policy
from src.discovery.registry import (
    SOURCE_INITIAL_RESEARCH_SNAPSHOT,
    STATUS_ACTIVE,
    STATUS_NEW,
    STATUS_QUEUED,
    RegistryEntry,
    build_onboarding_plan,
    reconcile_registry,
    seed_registry_from_snapshot,
)
from src.discovery.run_discovery import (
    apply_crawl_feasibility,
    build_feasibility_snapshot_id,
    qualify_candidates,
    run_control_plane,
)


AS_OF_DATE = date(2026, 9, 23)


def valid_candidate(appid, name=None):
    return {
        "appid": appid,
        "name": name or f"Game {appid}",
        "app_type": "game",
        "release_date": "2025-01-01",
        "total_reviews": 2000,
        "metadata_available": True,
        "review_endpoint_available": True,
    }


class FakeFeasibilityResult:
    def __init__(
        self,
        feasible,
        unique_reviews,
        status,
    ):
        self.feasible = feasible
        self.unique_reviews = unique_reviews
        self.status = status

    def to_dict(self):
        return {
            "feasible": self.feasible,
            "unique_reviews": self.unique_reviews,
            "status": self.status,
        }


class DiscoveryControlPlaneTest(unittest.TestCase):
    def setUp(self):
        self.policy = load_discovery_policy()

    def test_existing_active_appid_is_not_classified_new(self):
        registry = {
            10: RegistryEntry(10, STATUS_ACTIVE, "TEST", 1, name="Ten")
        }
        result = reconcile_registry(
            [valid_candidate(10)], registry, policy_version=1
        )
        self.assertEqual((10,), result.existing_active_appids)
        self.assertEqual((), result.newly_discovered_appids)
        self.assertEqual(STATUS_ACTIVE, result.registry[10].status)

    def test_qualified_unseen_appid_becomes_new(self):
        result = reconcile_registry(
            [valid_candidate(20)], {}, policy_version=1
        )
        self.assertEqual((20,), result.newly_discovered_appids)
        self.assertEqual(STATUS_NEW, result.registry[20].status)

    def test_duplicate_candidates_do_not_duplicate_registry_entries(self):
        result = reconcile_registry(
            [valid_candidate(30), valid_candidate(30)],
            {},
            policy_version=1,
        )
        self.assertEqual(1, len(result.registry))
        self.assertEqual((30,), result.newly_discovered_appids)

    def test_max_new_games_per_cycle_is_respected(self):
        candidates = [valid_candidate(appid) for appid in range(1, 14)]
        reconciliation = reconcile_registry(
            candidates, {}, policy_version=1
        )
        registry, plan = build_onboarding_plan(
            reconciliation,
            candidates,
            self.policy.onboarding,
            policy_version=1,
        )
        self.assertEqual(10, plan.queued_count)
        self.assertEqual(3, plan.deferred_count)
        self.assertTrue(
            all(registry[appid].status == STATUS_QUEUED for appid in range(1, 11))
        )

    def test_excess_new_games_remain_new_and_deferred(self):
        candidates = [valid_candidate(appid) for appid in range(1, 14)]
        reconciliation = reconcile_registry(
            candidates, {}, policy_version=1
        )
        registry, plan = build_onboarding_plan(
            reconciliation,
            candidates,
            self.policy.onboarding,
            policy_version=1,
        )
        self.assertEqual((11, 12, 13), plan.deferred_appids)
        self.assertTrue(
            all(registry[appid].status == STATUS_NEW for appid in plan.deferred_appids)
        )

    def test_rejected_games_do_not_enter_onboarding_plan(self):
        rejected = valid_candidate(40)
        rejected["total_reviews"] = 999
        result = run_control_plane(
            [valid_candidate(41), rejected],
            [],
            {},
            self.policy,
            current_date=AS_OF_DATE,
        )
        self.assertEqual((41,), result.onboarding_plan.queued_appids)
        self.assertNotIn(40, result.registry)

    def test_active_game_remains_active_after_policy_change(self):
        stricter_policy = replace(
            self.policy,
            version=2,
            qualification=replace(
                self.policy.qualification,
                min_total_reviews=5000,
            ),
        )
        registry = {
            50: RegistryEntry(50, STATUS_ACTIVE, "TEST", 1, name="Fifty")
        }
        result = run_control_plane(
            [valid_candidate(50)],
            [],
            registry,
            stricter_policy,
            current_date=AS_OF_DATE,
        )
        self.assertEqual(STATUS_ACTIVE, result.registry[50].status)
        self.assertEqual(1, result.registry[50].policy_version)

    def test_selected_50_games_seed_as_active_registry(self):
        snapshot = read_jsonl(
            self._project_root() / "data" / "raw" / "selected_50_games.jsonl"
        )
        registry = seed_registry_from_snapshot(
            snapshot, {}, policy_version=1
        )
        self.assertEqual(50, len(registry))
        self.assertTrue(
            all(entry.status == STATUS_ACTIVE for entry in registry.values())
        )
        self.assertTrue(
            all(
                entry.source == SOURCE_INITIAL_RESEARCH_SNAPSHOT
                for entry in registry.values()
            )
        )

    def test_individual_candidate_error_does_not_terminate_run(self):
        result = qualify_candidates(
            [{"appid": "invalid"}, valid_candidate(60)],
            self.policy,
            current_date=AS_OF_DATE,
        )
        self.assertEqual(1, result.failed_count)
        self.assertEqual(1, result.qualified_count)

    def test_discovery_run_report_counts_reconcile(self):
        rejected = valid_candidate(71)
        rejected["app_type"] = "dlc"
        result = run_control_plane(
            [valid_candidate(70), rejected, {"appid": None}],
            [],
            {},
            self.policy,
            current_date=AS_OF_DATE,
        )
        report = result.run_report
        self.assertEqual(3, report.candidate_count)
        self.assertEqual(1, report.qualified_count)
        self.assertEqual(1, report.rejected_count)
        self.assertEqual(1, report.failed_count)
        self.assertEqual(
            report.candidate_count,
            report.qualified_count + report.rejected_count + report.failed_count,
        )

    def test_feasibility_cache_resumes_after_interruption(self):
        qualification = qualify_candidates(
            [
                valid_candidate(101),
                valid_candidate(102),
                valid_candidate(103),
            ],
            self.policy,
            current_date=AS_OF_DATE,
        )

        with TemporaryDirectory() as directory:
            cache_path = (
                Path(directory)
                / "crawl_feasibility_cache.json"
            )

            first_calls = []

            def first_probe(
                session,
                appid,
                target_reviews,
                *,
                delay,
            ):
                first_calls.append(appid)

                if appid == 101:
                    return FakeFeasibilityResult(
                        True,
                        500,
                        "TARGET_REACHED",
                    )

                if appid == 102:
                    return FakeFeasibilityResult(
                        False,
                        120,
                        "EXHAUSTED",
                    )

                raise RuntimeError(
                    "simulated interruption"
                )

            with self.assertRaises(RuntimeError):
                apply_crawl_feasibility(
                    qualification,
                    500,
                    candidate_appids=[
                        101,
                        102,
                        103,
                    ],
                    required_feasible=2,
                    feasibility_probe=first_probe,
                    cache_path=cache_path,
                    cache_snapshot_id=(
                        "2026-W40:test-snapshot"
                    ),
                )

            self.assertEqual(
                [101, 102, 103],
                first_calls,
            )

            second_calls = []

            def second_probe(
                session,
                appid,
                target_reviews,
                *,
                delay,
            ):
                second_calls.append(appid)

                return FakeFeasibilityResult(
                    True,
                    500,
                    "TARGET_REACHED",
                )

            resumed = apply_crawl_feasibility(
                qualification,
                500,
                candidate_appids=[
                    101,
                    102,
                    103,
                ],
                required_feasible=2,
                feasibility_probe=second_probe,
                cache_path=cache_path,
                cache_snapshot_id=(
                    "2026-W40:test-snapshot"
                ),
            )

            self.assertEqual(
                [103],
                second_calls,
            )

            qualified_appids = {
                int(row["appid"])
                for row
                in resumed.qualified_candidates
            }

            rejected_appids = {
                int(row["appid"])
                for row
                in resumed.rejected_candidates
            }

            self.assertIn(
                101,
                qualified_appids,
            )
            self.assertIn(
                103,
                qualified_appids,
            )
            self.assertIn(
                102,
                rejected_appids,
            )

    def test_feasibility_cache_is_scoped_to_iso_week(self):
        qualification = qualify_candidates(
            [valid_candidate(201)],
            self.policy,
            current_date=AS_OF_DATE,
        )

        with TemporaryDirectory() as directory:
            root = Path(directory)

            source_paths = [
                root / "candidates.jsonl",
                root / "metadata.jsonl",
                root / "reviews.jsonl",
            ]

            for index, source_path in enumerate(
                source_paths,
                start=1,
            ):
                source_path.write_text(
                    f"fixture-{index}\n",
                    encoding="utf-8",
                )

            week_40 = build_feasibility_snapshot_id(
                source_paths,
                snapshot_date=date(
                    2026,
                    10,
                    2,
                ),
            )

            same_week = build_feasibility_snapshot_id(
                source_paths,
                snapshot_date=date(
                    2026,
                    10,
                    3,
                ),
            )

            week_41 = build_feasibility_snapshot_id(
                source_paths,
                snapshot_date=date(
                    2026,
                    10,
                    9,
                ),
            )

            self.assertEqual(
                week_40,
                same_week,
            )
            self.assertNotEqual(
                week_40,
                week_41,
            )

            self.assertEqual(
                week_40.split(":", 1)[1],
                week_41.split(":", 1)[1],
            )

            cache_path = (
                root
                / "crawl_feasibility_cache.json"
            )

            warm_calls = []

            def warm_probe(
                session,
                appid,
                target_reviews,
                *,
                delay,
            ):
                warm_calls.append(appid)

                return FakeFeasibilityResult(
                    True,
                    500,
                    "TARGET_REACHED",
                )

            apply_crawl_feasibility(
                qualification,
                500,
                candidate_appids=[201],
                required_feasible=1,
                feasibility_probe=warm_probe,
                cache_path=cache_path,
                cache_snapshot_id=week_40,
            )

            self.assertEqual(
                [201],
                warm_calls,
            )

            def must_not_probe(
                session,
                appid,
                target_reviews,
                *,
                delay,
            ):
                raise AssertionError(
                    "same-week cache was not reused"
                )

            apply_crawl_feasibility(
                qualification,
                500,
                candidate_appids=[201],
                required_feasible=1,
                feasibility_probe=must_not_probe,
                cache_path=cache_path,
                cache_snapshot_id=same_week,
            )

            new_week_calls = []

            def new_week_probe(
                session,
                appid,
                target_reviews,
                *,
                delay,
            ):
                new_week_calls.append(appid)

                return FakeFeasibilityResult(
                    True,
                    500,
                    "TARGET_REACHED",
                )

            apply_crawl_feasibility(
                qualification,
                500,
                candidate_appids=[201],
                required_feasible=1,
                feasibility_probe=new_week_probe,
                cache_path=cache_path,
                cache_snapshot_id=week_41,
            )

            self.assertEqual(
                [201],
                new_week_calls,
            )

    @staticmethod
    def _project_root():
        from pathlib import Path

        return Path(__file__).resolve().parents[1]


if __name__ == "__main__":
    unittest.main()
