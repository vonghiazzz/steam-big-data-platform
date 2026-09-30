import tempfile
import unittest
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from src.serving.realtime_mongodb_sink import (
    build_realtime_game_metrics,
    build_realtime_metric_document,
    build_recent_review_document,
    prepare_recent_reviews,
    realtime_metric_id,
)


UTC = timezone.utc


def review_row():
    return {
        "recommendationid": "review-123",
        "appid": 730,
        "voted_up": True,
        "playtime_at_review": 120,
        "playtime_forever": 240,
        "steam_purchase": True,
        "received_for_free": False,
        "timestamp_created": 1_790_640_000,
        "stream_ingested_at": "2026-09-29T00:00:05Z",
    }


def metric_row():
    return {
        "appid": 730,
        "window_start": datetime(2026, 9, 29, 0, 0, tzinfo=UTC),
        "window_end": datetime(2026, 9, 29, 1, 0, tzinfo=UTC),
        "review_count": 10,
        "positive_reviews": 7,
        "negative_reviews": 3,
        "recommendation_rate": 0.7,
    }


class RealtimeMongoDocumentTest(unittest.TestCase):
    def test_recent_review_id_is_recommendationid(self):
        document = build_recent_review_document(review_row())
        self.assertEqual(document["_id"], "review-123")
        self.assertEqual(document["_id"], document["recommendationid"])

    def test_realtime_window_id_is_deterministic(self):
        row = metric_row()
        identity = realtime_metric_id(
            row["appid"], row["window_start"], row["window_end"]
        )
        self.assertEqual(
            identity,
            "730:2026-09-29T00:00:00Z:2026-09-29T01:00:00Z",
        )

    def test_same_review_retry_has_same_identity(self):
        first = build_recent_review_document(review_row())
        second = build_recent_review_document(review_row())
        self.assertEqual(first["_id"], second["_id"])

    def test_same_metric_retry_has_same_identity(self):
        first = build_realtime_metric_document(metric_row())
        second = build_realtime_metric_document(metric_row())
        self.assertEqual(first["_id"], second["_id"])

    def test_numeric_fields_remain_numeric(self):
        review = build_recent_review_document(review_row())
        metric = build_realtime_metric_document(metric_row())
        self.assertIsInstance(review["appid"], int)
        self.assertIsInstance(review["playtime_at_review"], int)
        self.assertIsInstance(metric["review_count"], int)
        self.assertIsInstance(metric["recommendation_rate"], float)

    def test_boolean_fields_remain_boolean(self):
        document = build_recent_review_document(review_row())
        for field in (
            "voted_up",
            "steam_purchase",
            "received_for_free",
        ):
            self.assertIsInstance(document[field], bool)

    def test_timestamp_conversion_produces_queryable_utc_datetimes(self):
        document = build_recent_review_document(review_row())
        self.assertIsInstance(document["timestamp_created"], datetime)
        self.assertIsInstance(document["stream_ingested_at"], datetime)
        self.assertEqual(document["timestamp_created"].tzinfo, UTC)
        self.assertEqual(document["stream_ingested_at"].tzinfo, UTC)

    def test_recommendation_rate_is_calculated_from_counts(self):
        row = metric_row()
        row["recommendation_rate"] = 0.1
        document = build_realtime_metric_document(row)
        self.assertEqual(document["recommendation_rate"], 0.7)

    def test_positive_and_negative_must_equal_review_count(self):
        row = metric_row()
        row["negative_reviews"] = 2
        with self.assertRaisesRegex(ValueError, "must equal review_count"):
            build_realtime_metric_document(row)

    def test_identities_are_plain_deterministic_strings(self):
        review = build_recent_review_document(review_row())
        metric = build_realtime_metric_document(metric_row())
        self.assertIs(type(review["_id"]), str)
        self.assertIs(type(metric["_id"]), str)


class RealtimeMongoWindowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("Realtime MongoDB one-hour window test")
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

    def test_one_hour_tumbling_window_semantics(self):
        silver = self.spark.createDataFrame(
            [
                ("r-1", 730, True, 10, 20, True, False, 1_790_640_010),
                ("r-2", 730, False, 30, 40, True, False, 1_790_640_020),
            ],
            """
            recommendationid string, appid int, voted_up boolean,
            playtime_at_review int, playtime_forever int,
            steam_purchase boolean, received_for_free boolean,
            timestamp_created long
            """,
        )
        recent = prepare_recent_reviews(silver)
        recent_documents = [
            build_recent_review_document(row)
            for row in recent.drop("event_time_ts").collect()
        ]
        self.assertEqual(len(recent_documents), 2)
        self.assertTrue(
            all(
                isinstance(document["timestamp_created"], datetime)
                for document in recent_documents
            )
        )
        rows = build_realtime_game_metrics(recent, "7 days").collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].review_count, 2)
        self.assertEqual(rows[0].positive_reviews, 1)
        self.assertEqual(rows[0].negative_reviews, 1)
        self.assertEqual(rows[0].recommendation_rate, 0.5)
        self.assertEqual(
            int((rows[0].window_end - rows[0].window_start).total_seconds()),
            3600,
        )


if __name__ == "__main__":
    unittest.main()
