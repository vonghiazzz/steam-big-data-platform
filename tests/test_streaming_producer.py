import json
import tempfile
import unittest
from pathlib import Path

from src.streaming.producer_state import ProducerState
from src.streaming.review_producer import (
    ActiveScopeSafetyError,
    ActiveScopeTracker,
    LocalJsonlReviewSource,
    ReviewProducerService,
    load_active_scope,
)


def review(recommendationid: str, timestamp_created: int) -> dict:
    return {
        "recommendationid": recommendationid,
        "timestamp_created": timestamp_created,
        "voted_up": True,
        "author": {
            "playtime_at_review": 120,
            "playtime_forever": 180,
        },
    }


class FakeClient:
    def __init__(self, reviews):
        self.reviews = reviews
        self.calls = []

    def fetch_page(self, appid, cursor):
        self.calls.append((appid, cursor))
        return {"success": 1, "reviews": list(self.reviews), "cursor": ""}


class FakePublisher:
    def __init__(self):
        self.events = []

    def send(self, appid, event):
        self.events.append((appid, dict(event)))

    def flush(self):
        return None

    def close(self):
        return None


class FakeBaselineSource:
    def __init__(self, reviews_by_appid):
        self.reviews_by_appid = reviews_by_appid
        self.calls = []

    def load_reviews(self, appid):
        self.calls.append(appid)
        return list(self.reviews_by_appid.get(appid, []))


class PerGameClient:
    def __init__(self, reviews_by_appid):
        self.reviews_by_appid = reviews_by_appid
        self.calls = []

    def fetch_page(self, appid, cursor):
        self.calls.append((appid, cursor))
        return {
            "success": 1,
            "reviews": list(self.reviews_by_appid.get(appid, [])),
            "cursor": "",
        }


def write_registry(path: Path, statuses: dict[int, str]) -> None:
    path.write_text(
        "".join(
            json.dumps(
                {
                    "appid": appid,
                    "status": status,
                    "source": "TEST",
                    "policy_version": 1,
                }
            )
            + "\n"
            for appid, status in sorted(statuses.items())
        ),
        encoding="utf-8",
    )


class ProducerV1Test(unittest.TestCase):
    def test_bootstrap_no_new_duplicate_and_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_path = root / "state.sqlite3"
            canonical = root / "canonical"
            canonical.mkdir()
            client = FakeClient([review("old-2", 200), review("old-1", 100)])
            publisher = FakePublisher()

            with ProducerState(state_path) as state:
                service = ReviewProducerService(
                    state=state,
                    client=client,
                    publisher=publisher,
                    max_pages=1,
                    request_delay_seconds=0,
                    baseline_source=LocalJsonlReviewSource(
                        canonical,
                        max_ids_per_game=1000,
                    ),
                    clock=lambda: "2026-09-29T00:00:00+00:00",
                )

                first = service.poll_game(570)
                self.assertTrue(first.bootstrap)
                self.assertEqual(first.emitted, 0)
                self.assertEqual(publisher.events, [])

                unchanged = service.poll_game(570)
                self.assertFalse(unchanged.bootstrap)
                self.assertEqual(unchanged.emitted, 0)

                client.reviews = [review("new-1", 300), review("old-2", 200)]
                changed = service.poll_game(570)
                self.assertEqual(changed.emitted, 1)
                self.assertEqual(
                    publisher.events[0][1]["event_id"],
                    "REVIEW_CREATED:570:new-1",
                )

                duplicate = service.poll_game(570)
                self.assertEqual(duplicate.emitted, 0)

            with ProducerState(state_path) as restarted_state:
                restarted = ReviewProducerService(
                    state=restarted_state,
                    client=client,
                    publisher=publisher,
                    max_pages=1,
                    request_delay_seconds=0,
                    baseline_source=LocalJsonlReviewSource(
                        canonical,
                        max_ids_per_game=1000,
                    ),
                    clock=lambda: "2026-09-29T00:01:00+00:00",
                )
                after_restart = restarted.poll_game(570)
                self.assertEqual(after_restart.emitted, 0)
                self.assertEqual(len(publisher.events), 1)

    def test_registry_scope_contains_active_only(self):
        with tempfile.TemporaryDirectory() as directory:
            registry = Path(directory) / "registry.jsonl"
            rows = [
                {
                    "appid": 10,
                    "status": "ACTIVE",
                    "source": "TEST",
                    "policy_version": 1,
                },
                {
                    "appid": 20,
                    "status": "NEW",
                    "source": "TEST",
                    "policy_version": 1,
                },
                {
                    "appid": 30,
                    "status": "RETIRED",
                    "source": "TEST",
                    "policy_version": 1,
                },
            ]
            registry.write_text(
                "".join(json.dumps(row) + "\n" for row in rows),
                encoding="utf-8",
            )
            active, counts = load_active_scope(registry)
            self.assertEqual([entry.appid for entry in active], [10])
            self.assertEqual(counts, {"total": 3, "active": 1, "retired": 1})

    def test_scope_reload_detects_additions_removals_and_unchanged_registry(self):
        with tempfile.TemporaryDirectory() as directory:
            registry = Path(directory) / "registry.jsonl"
            write_registry(registry, {10: "ACTIVE", 20: "NEW"})
            tracker = ActiveScopeTracker(
                registry_path=registry,
                max_active_games=10,
            )

            initial = tracker.reload()
            unchanged = tracker.reload()
            write_registry(registry, {10: "ACTIVE", 20: "ACTIVE"})
            expanded = tracker.reload()
            write_registry(registry, {10: "PAUSED", 20: "ACTIVE"})
            reduced = tracker.reload()

            self.assertEqual((10,), initial.added_appids)
            self.assertEqual((10,), tuple(entry.appid for entry in initial.entries))
            self.assertEqual((), unchanged.added_appids)
            self.assertEqual((), unchanged.removed_appids)
            self.assertEqual((20,), expanded.added_appids)
            self.assertEqual((10,), reduced.removed_appids)
            self.assertEqual((20,), tuple(entry.appid for entry in reduced.entries))

    def test_new_active_game_bootstraps_without_emitting_history_then_emits_new(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = root / "registry.jsonl"
            write_registry(registry, {10: "ACTIVE", 20: "NEW"})
            tracker = ActiveScopeTracker(
                registry_path=registry,
                max_active_games=10,
            )
            tracker.reload()

            baseline = FakeBaselineSource(
                {
                    10: [review("history-10", 100)],
                    20: [review("history-20", 100)],
                }
            )
            client = PerGameClient(
                {
                    10: [review("history-10", 100)],
                    20: [review("history-20", 100)],
                }
            )
            publisher = FakePublisher()
            with ProducerState(root / "state.sqlite3") as state:
                service = ReviewProducerService(
                    state=state,
                    client=client,
                    publisher=publisher,
                    max_pages=1,
                    request_delay_seconds=0,
                    baseline_source=baseline,
                    clock=lambda: "2026-10-01T00:00:00+00:00",
                )
                service.poll_game(10)

                write_registry(registry, {10: "ACTIVE", 20: "ACTIVE"})
                expanded = tracker.reload()
                first_new_game_poll = service.poll_game(20)
                unchanged = tracker.reload()
                second_new_game_poll = service.poll_game(20)

                self.assertEqual((20,), expanded.added_appids)
                self.assertEqual((), unchanged.added_appids)
                self.assertTrue(first_new_game_poll.bootstrap)
                self.assertEqual(0, first_new_game_poll.emitted)
                self.assertEqual(0, second_new_game_poll.emitted)
                self.assertEqual(1, baseline.calls.count(20))
                self.assertTrue(state.has_seen(20, "history-20"))
                self.assertEqual([], publisher.events)

                client.reviews_by_appid[20] = [
                    review("future-20", 200),
                    review("history-20", 100),
                ]
                future = service.poll_game(20)
                self.assertEqual(1, future.emitted)
                self.assertEqual(
                    "REVIEW_CREATED:20:future-20",
                    publisher.events[0][1]["event_id"],
                )

    def test_removed_game_is_not_polled_and_its_state_is_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = root / "registry.jsonl"
            write_registry(registry, {10: "ACTIVE", 20: "ACTIVE"})
            tracker = ActiveScopeTracker(
                registry_path=registry,
                max_active_games=10,
            )
            tracker.reload()
            baseline = FakeBaselineSource({10: [review("history-10", 100)]})
            client = PerGameClient({10: [review("history-10", 100)], 20: []})
            publisher = FakePublisher()

            with ProducerState(root / "state.sqlite3") as state:
                service = ReviewProducerService(
                    state=state,
                    client=client,
                    publisher=publisher,
                    max_pages=1,
                    request_delay_seconds=0,
                    baseline_source=baseline,
                )
                service.poll_game(10)
                calls_before_removal = len(client.calls)

                write_registry(registry, {10: "PAUSED", 20: "ACTIVE"})
                reduced = tracker.reload()
                for entry in reduced.entries:
                    service.poll_game(entry.appid)

                self.assertEqual((10,), reduced.removed_appids)
                self.assertNotIn(
                    10,
                    [appid for appid, _cursor in client.calls[calls_before_removal:]],
                )
                self.assertTrue(state.is_initialized(10))
                self.assertTrue(state.has_seen(10, "history-10"))

    def test_scope_limit_refuses_expansion_without_replacing_current_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            registry = Path(directory) / "registry.jsonl"
            write_registry(registry, {10: "ACTIVE", 20: "NEW"})
            tracker = ActiveScopeTracker(
                registry_path=registry,
                max_active_games=1,
            )
            tracker.reload()
            write_registry(registry, {10: "ACTIVE", 20: "ACTIVE"})

            with self.assertRaisesRegex(ActiveScopeSafetyError, "exceeds"):
                tracker.reload()

            self.assertEqual((10,), tuple(entry.appid for entry in tracker.entries))

    def test_registry_reload_failure_preserves_scope_and_producer_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = root / "registry.jsonl"
            write_registry(registry, {10: "ACTIVE"})
            tracker = ActiveScopeTracker(
                registry_path=registry,
                max_active_games=10,
            )
            tracker.reload()

            with ProducerState(root / "state.sqlite3") as state:
                state.complete_bootstrap(
                    10,
                    [review("history-10", 100)],
                    polled_at="2026-10-01T00:00:00+00:00",
                )
                registry.write_text("not-json\n", encoding="utf-8")

                with self.assertRaises(Exception):
                    tracker.reload()

                self.assertEqual(
                    (10,), tuple(entry.appid for entry in tracker.entries)
                )
                self.assertTrue(state.is_initialized(10))
                self.assertTrue(state.has_seen(10, "history-10"))


if __name__ == "__main__":
    unittest.main()
