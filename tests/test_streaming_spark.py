import json
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timezone
from pathlib import Path

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

from src.gold.gold_join import create_gold_dataset
from src.streaming.review_streaming import (
    build_silver_reviews,
    parse_and_classify_events,
    quarantine_projection,
    remove_batch_baseline_ids,
    resolve_historical_silver_paths,
    resolve_stream_paths,
)


class StreamingSparkV1Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("Streaming V1 bounded fixture test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.warehouse.dir", cls.temp.name)
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.temp.cleanup()

    def test_one_valid_event_and_one_malformed_event(self):
        event = {
            "event_id": "REVIEW_CREATED:570:fixture-1",
            "event_type": "REVIEW_CREATED",
            "appid": 570,
            "event_time": "2026-09-29T00:00:00+00:00",
            "produced_at": "2026-09-29T00:00:01+00:00",
            "payload": {
                "recommendationid": "fixture-1",
                "author": {
                    "steamid": "fixture-user",
                    "playtime_forever": 240,
                    "playtime_at_review": 120,
                },
                "language": "english",
                "review": "test-only fixture",
                "timestamp_created": 1790640000,
                "timestamp_updated": 1790640000,
                "voted_up": True,
                "votes_up": 1,
                "votes_funny": 0,
                "weighted_vote_score": "0.5",
                "steam_purchase": True,
                "received_for_free": False,
            },
        }
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
        now = datetime(2026, 9, 29, tzinfo=timezone.utc)
        kafka = self.spark.createDataFrame(
            [
                (
                    b"570",
                    json.dumps(event).encode("utf-8"),
                    "steam_events",
                    0,
                    1,
                    now,
                ),
                (b"570", b"{malformed", "steam_events", 0, 2, now),
            ],
            kafka_schema,
        )
        games = self.spark.createDataFrame(
            [
                (
                    570,
                    "Dota 2",
                    True,
                    0.0,
                    None,
                    ["Action"],
                    ["Multi-player"],
                    (True, True, True),
                    "9 Jul, 2013",
                )
            ],
            """
            appid int, game_name string, is_free boolean, price double,
            currency string, genres array<string>, categories array<string>,
            platforms struct<windows:boolean,mac:boolean,linux:boolean>,
            release_date string
            """,
        )

        classified = parse_and_classify_events(kafka, games)
        valid = classified.filter("error_reason = ''")
        invalid = quarantine_projection(classified)
        self.assertEqual(valid.count(), 1)
        self.assertEqual(invalid.count(), 1)
        self.assertIn("MISSING_EVENT_ID", invalid.first().error_reason)

        silver = build_silver_reviews(valid)
        baseline = self.spark.createDataFrame([], "recommendationid string")
        silver = remove_batch_baseline_ids(silver, baseline)
        self.assertEqual(silver.count(), 1)
        self.assertEqual(silver.first().recommendationid, "fixture-1")

        gold = create_gold_dataset(silver, games)
        self.assertEqual(gold.count(), 1)
        self.assertEqual(gold.first().recommendationid, "fixture-1")
        self.assertNotIn("review", gold.columns)
        self.assertNotIn("overall_positive_rate", gold.columns)
        self.assertNotIn("primary_genre", gold.columns)
        self.assertNotIn("package_groups", gold.columns)

    def test_fixture_paths_are_isolated_from_production(self):
        with patch.dict(
            "os.environ",
            {
                "STREAM_HDFS_ROOT": "/steam/test/streaming_v1/",
                "STREAM_CHECKPOINT_ROOT": "/steam/test/checkpoints/",
            },
        ):
            paths = resolve_stream_paths()
        self.assertEqual(
            paths.bronze_events,
            "/steam/test/streaming_v1/bronze/stream_events",
        )
        self.assertEqual(
            paths.silver_reviews,
            "/steam/test/streaming_v1/silver/reviews_incremental_v1",
        )
        self.assertEqual(
            paths.gold_base,
            "/steam/test/streaming_v1/gold/base_incremental_v1",
        )
        self.assertEqual(
            paths.quarantine,
            "/steam/test/streaming_v1/quarantine/review_events",
        )
        self.assertEqual(
            paths.silver_checkpoint,
            "/steam/test/checkpoints/review_silver_v1",
        )
        self.assertEqual(
            paths.mongodb_recent_checkpoint,
            "/steam/test/checkpoints/mongodb/recent_reviews",
        )
        self.assertEqual(
            paths.mongodb_metrics_checkpoint,
            "/steam/test/checkpoints/mongodb/realtime_game_metrics",
        )

    def test_historical_silver_paths_can_be_overridden_for_local_runs(self):
        with patch.dict(
            "os.environ",
            {
                "STREAM_SILVER_GAMES_PATH": "file:///tmp/silver/games",
                "STREAM_SILVER_REVIEWS_PATH": "file:///tmp/silver/reviews",
            },
        ):
            self.assertEqual(
                resolve_historical_silver_paths(),
                (
                    "file:///tmp/silver/games",
                    "file:///tmp/silver/reviews",
                ),
            )

    def test_file_stream_checkpoint_restart_does_not_replay(self):
        root = Path(self.temp.name) / "checkpoint-restart"
        source = root / "source"
        output = root / "output"
        checkpoint = root / "checkpoint"
        self.spark.createDataFrame(
            [("restart-fixture",)],
            "recommendationid string",
        ).write.mode("append").json(str(source))

        def run_once():
            query = (
                self.spark.readStream.schema("recommendationid string")
                .json(str(source))
                .writeStream.format("parquet")
                .option("path", str(output))
                .option("checkpointLocation", str(checkpoint))
                .trigger(availableNow=True)
                .start()
            )
            query.awaitTermination()

        run_once()
        run_once()
        rows = self.spark.read.parquet(str(output)).collect()
        self.assertEqual([row.recommendationid for row in rows], ["restart-fixture"])


if __name__ == "__main__":
    unittest.main()
