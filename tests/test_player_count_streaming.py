import json
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.streaming.player_count_streaming import (
    build_player_count_gold,
    parse_and_classify_player_count_events,
    player_count_quarantine_projection,
    resolve_player_count_paths,
)


class PlayerCountStreamingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("Player count streaming unit test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.warehouse.dir", cls.temp.name)
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.temp.cleanup()

    def test_valid_unknown_and_malformed_events(self):
        event = {
            "event_id": "PLAYER_COUNT_SNAPSHOT:570:2026-10-01T00:00:00+00:00",
            "event_type": "PLAYER_COUNT_SNAPSHOT",
            "schema_version": 1,
            "appid": 570,
            "event_time": "2026-10-01T00:00:00+00:00",
            "produced_at": "2026-10-01T00:00:01+00:00",
            "payload": {"player_count": 123456},
        }
        unknown = dict(event)
        unknown["event_id"] = (
            "PLAYER_COUNT_SNAPSHOT:999999:2026-10-01T00:00:00+00:00"
        )
        unknown["appid"] = 999999
        kafka_schema = StructType(
            [
                StructField("key", BinaryType(), True),
                StructField("value", BinaryType(), True),
                StructField("topic", StringType(), False),
                StructField("partition", IntegerType(), False),
                StructField("offset", LongType(), False),
                StructField("timestamp", TimestampType(), False),
            ]
        )
        now = datetime(2026, 10, 1, tzinfo=timezone.utc)
        kafka = self.spark.createDataFrame(
            [
                (
                    b"570",
                    json.dumps(event).encode("utf-8"),
                    "steam_player_events",
                    0,
                    1,
                    now,
                ),
                (
                    b"999999",
                    json.dumps(unknown).encode("utf-8"),
                    "steam_player_events",
                    0,
                    2,
                    now,
                ),
                (
                    b"570",
                    b"{malformed",
                    "steam_player_events",
                    0,
                    3,
                    now,
                ),
            ],
            kafka_schema,
        )
        games = self.spark.createDataFrame(
            [(570, "Dota 2")],
            "appid int, game_name string",
        )

        classified = parse_and_classify_player_count_events(kafka, games)
        valid = classified.filter("error_reason = ''")
        invalid = player_count_quarantine_projection(classified)
        self.assertEqual(valid.count(), 1)
        self.assertEqual(valid.first().player_count, 123456)
        reasons = {row.error_reason for row in invalid.collect()}
        self.assertEqual(reasons, {"UNKNOWN_APPID", "MISSING_EVENT_ID"})

        silver_fixture = valid.select(
            "event_id",
            "appid",
            "game_name",
            "player_count",
            valid.event_time_ts.alias("observed_at"),
            valid.produced_at_ts.alias("produced_at"),
            valid.processing_timestamp.alias("stream_ingested_at"),
            valid.ingest_date.alias("snapshot_date"),
        )
        gold = build_player_count_gold(silver_fixture)
        self.assertEqual(gold.count(), 1)
        self.assertIn("snapshot_hour", gold.columns)

    def test_paths_are_isolated_from_review_streaming(self):
        with patch.dict(
            "os.environ",
            {
                "PLAYER_COUNT_HDFS_ROOT": "/steam/test/player_count/",
                "PLAYER_COUNT_CHECKPOINT_ROOT": "/steam/test/pc-checkpoints/",
            },
        ):
            paths = resolve_player_count_paths()
        self.assertEqual(
            paths.bronze_events,
            "/steam/test/player_count/bronze/player_count_events",
        )
        self.assertEqual(
            paths.silver_snapshots,
            "/steam/test/player_count/silver/player_count_snapshots_v1",
        )
        self.assertEqual(
            paths.mongodb_checkpoint,
            "/steam/test/pc-checkpoints/mongodb/player_count_latest_v1",
        )


if __name__ == "__main__":
    unittest.main()
