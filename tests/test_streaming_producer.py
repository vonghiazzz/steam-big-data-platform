import json
import tempfile
import unittest
from pathlib import Path

from src.streaming.producer_state import ProducerState
from src.streaming.review_producer import (
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


if __name__ == "__main__":
    unittest.main()
