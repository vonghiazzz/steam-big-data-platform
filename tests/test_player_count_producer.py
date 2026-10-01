import json
import tempfile
import unittest
from pathlib import Path

from src.streaming.event_contract import build_player_count_snapshot_event
from src.streaming.player_count_producer import (
    PlayerCountProducerService,
    load_active_scope,
)
from src.streaming.player_count_state import PlayerCountState


class FakeClient:
    def __init__(self, counts):
        self.counts = iter(counts)
        self.calls = []

    def fetch_count(self, appid):
        self.calls.append(appid)
        return next(self.counts)


class FakePublisher:
    def __init__(self, *, fail_once=False):
        self.events = []
        self.fail_once = fail_once

    def send(self, appid, event):
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("simulated Kafka outage")
        self.events.append((appid, dict(event)))

    def flush(self):
        return None

    def close(self):
        return None


class SequenceClock:
    def __init__(self):
        self.index = 0

    def __call__(self):
        self.index += 1
        return f"2026-10-01T00:00:{self.index:02d}+00:00"


class PlayerCountProducerTest(unittest.TestCase):
    def test_event_contract_is_deterministic_and_validated(self):
        event = build_player_count_snapshot_event(
            570,
            123456,
            observed_at="2026-10-01T07:00:00+07:00",
            produced_at="2026-10-01T00:00:01Z",
        )
        self.assertEqual(event["event_type"], "PLAYER_COUNT_SNAPSHOT")
        self.assertEqual(event["schema_version"], 1)
        self.assertEqual(event["event_time"], "2026-10-01T00:00:00+00:00")
        self.assertEqual(
            event["event_id"],
            "PLAYER_COUNT_SNAPSHOT:570:2026-10-01T00:00:00+00:00",
        )
        self.assertEqual(event["payload"]["player_count"], 123456)
        with self.assertRaisesRegex(ValueError, "non-negative"):
            build_player_count_snapshot_event(
                570,
                -1,
                observed_at="2026-10-01T00:00:00Z",
                produced_at="2026-10-01T00:00:01Z",
            )

    def test_each_poll_emits_a_time_series_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            publisher = FakePublisher()
            with PlayerCountState(Path(directory) / "state.sqlite3") as state:
                service = PlayerCountProducerService(
                    state=state,
                    client=FakeClient([100, 100]),
                    publisher=publisher,
                    clock=SequenceClock(),
                )
                first = service.poll_game(570)
                second = service.poll_game(570)
            self.assertEqual(first.player_count, 100)
            self.assertEqual(second.player_count, 100)
            self.assertEqual(len(publisher.events), 2)
            self.assertNotEqual(
                publisher.events[0][1]["event_id"],
                publisher.events[1][1]["event_id"],
            )

    def test_outbox_replays_after_publish_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.sqlite3"
            with PlayerCountState(state_path) as state:
                service = PlayerCountProducerService(
                    state=state,
                    client=FakeClient([200]),
                    publisher=FakePublisher(fail_once=True),
                    clock=SequenceClock(),
                )
                with self.assertRaisesRegex(RuntimeError, "Kafka outage"):
                    service.poll_game(730)
                self.assertEqual(len(state.pending_events(730)), 1)

            publisher = FakePublisher()
            with PlayerCountState(state_path) as restarted_state:
                restarted = PlayerCountProducerService(
                    state=restarted_state,
                    client=FakeClient([201]),
                    publisher=publisher,
                    clock=SequenceClock(),
                )
                result = restarted.poll_game(730)
                self.assertEqual(result.emitted, 2)
                self.assertEqual(restarted_state.pending_events(730), [])
            self.assertEqual(
                [event[1]["payload"]["player_count"] for event in publisher.events],
                [200, 201],
            )

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
            ]
            registry.write_text(
                "".join(json.dumps(row) + "\n" for row in rows),
                encoding="utf-8",
            )
            active, counts = load_active_scope(registry)
            self.assertEqual([entry.appid for entry in active], [10])
            self.assertEqual(counts["active"], 1)


if __name__ == "__main__":
    unittest.main()
