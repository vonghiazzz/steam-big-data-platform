import unittest
from datetime import datetime, timezone

from src.serving.player_count_mongodb_sink import (
    build_player_count_latest_document,
)


class PlayerCountMongoDocumentTest(unittest.TestCase):
    def test_document_uses_appid_identity_and_numeric_count(self):
        document = build_player_count_latest_document(
            {
                "event_id": (
                    "PLAYER_COUNT_SNAPSHOT:570:2026-10-01T00:00:00+00:00"
                ),
                "appid": 570,
                "game_name": "Dota 2",
                "player_count": 123456,
                "observed_at": datetime(2026, 10, 1, tzinfo=timezone.utc),
                "stream_ingested_at": "2026-10-01T00:00:05Z",
            }
        )
        self.assertEqual(document["_id"], 570)
        self.assertEqual(document["appid"], 570)
        self.assertEqual(document["player_count"], 123456)
        self.assertIsInstance(document["observed_at"], datetime)
        self.assertEqual(document["observed_at"].tzinfo, timezone.utc)

    def test_negative_count_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "non-negative"):
            build_player_count_latest_document(
                {
                    "event_id": "event",
                    "appid": 570,
                    "game_name": "Dota 2",
                    "player_count": -1,
                    "observed_at": "2026-10-01T00:00:00Z",
                    "stream_ingested_at": "2026-10-01T00:00:05Z",
                }
            )

    def test_epoch_microseconds_override_timezone_naive_spark_datetimes(self):
        document = build_player_count_latest_document(
            {
                "event_id": "event",
                "appid": 570,
                "game_name": "Dota 2",
                "player_count": 123456,
                "observed_at": datetime(2026, 10, 1, 7, 0, 0),
                "stream_ingested_at": datetime(2026, 10, 1, 7, 0, 5),
                "_observed_at_epoch_micros": 1790838000000000,
                "_stream_ingested_at_epoch_micros": 1790838005000000,
            }
        )
        self.assertEqual(
            document["observed_at"],
            datetime(2026, 10, 1, 7, 0, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(
            document["stream_ingested_at"],
            datetime(2026, 10, 1, 7, 0, 5, tzinfo=timezone.utc),
        )


if __name__ == "__main__":
    unittest.main()
