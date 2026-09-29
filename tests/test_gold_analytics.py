import tempfile
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.analytics.gold_analytics import (
    engagement_by_game,
    free_vs_paid,
    label_profile,
    recommendation_by_category,
    recommendation_by_game,
    recommendation_by_genre,
    recommendation_by_platform,
    recommendation_by_playtime,
    recommendation_by_purchase_source,
)


PLATFORMS_SCHEMA = StructType(
    [
        StructField("windows", BooleanType(), True),
        StructField("mac", BooleanType(), True),
        StructField("linux", BooleanType(), True),
    ]
)

GOLD_ANALYTICS_SCHEMA = StructType(
    [
        StructField("recommendationid", StringType(), False),
        StructField("appid", IntegerType(), False),
        StructField("game_name", StringType(), False),
        StructField("voted_up", BooleanType(), False),
        StructField("playtime_hours", DoubleType(), True),
        StructField("playtime_bucket", StringType(), True),
        StructField("playtime_at_review", LongType(), True),
        StructField("playtime_forever", LongType(), True),
        StructField("is_free", BooleanType(), False),
        StructField("price", DoubleType(), True),
        StructField("genres", ArrayType(StringType()), True),
        StructField("categories", ArrayType(StringType()), True),
        StructField("steam_purchase", BooleanType(), True),
        StructField("received_for_free", BooleanType(), True),
        StructField("platforms", PLATFORMS_SCHEMA, True),
        StructField("votes_up", LongType(), True),
        StructField("votes_funny", LongType(), True),
        StructField("weighted_vote_score", StringType(), True),
        StructField("timestamp_created", LongType(), True),
    ]
)


def rows_by(result_df, key):
    return {
        row[key]: row.asDict(recursive=True)
        for row in result_df.collect()
    }


class GoldAnalyticsV11Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("Gold Analytics V1.1 unit tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.warehouse.dir", cls.temp.name)
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.gold = cls.spark.createDataFrame(
            [
                {
                    "recommendationid": "r1",
                    "appid": 10,
                    "game_name": "Paid A",
                    "voted_up": True,
                    "playtime_hours": 1.0,
                    "playtime_bucket": "0-2h",
                    "playtime_at_review": 60,
                    "playtime_forever": 120,
                    "is_free": False,
                    "price": 100.0,
                    "genres": ["Action", "RPG", "Action"],
                    "categories": [
                        "Single-player",
                        "Achievements",
                        "Achievements",
                    ],
                    "steam_purchase": True,
                    "received_for_free": False,
                    "platforms": {
                        "windows": True,
                        "mac": True,
                        "linux": False,
                    },
                    "votes_up": 4,
                    "votes_funny": 1,
                    "weighted_vote_score": "0.75",
                    "timestamp_created": 100,
                },
                {
                    "recommendationid": "r2",
                    "appid": 10,
                    "game_name": "Paid A",
                    "voted_up": False,
                    "playtime_hours": 3.0,
                    "playtime_bucket": "2-10h",
                    "playtime_at_review": 180,
                    "playtime_forever": 240,
                    "is_free": False,
                    "price": 100.0,
                    "genres": ["Action", "RPG", "Action"],
                    "categories": [
                        "Single-player",
                        "Achievements",
                        "Achievements",
                    ],
                    "steam_purchase": False,
                    "received_for_free": False,
                    "platforms": {
                        "windows": True,
                        "mac": True,
                        "linux": False,
                    },
                    "votes_up": 2,
                    "votes_funny": 3,
                    "weighted_vote_score": "0.25",
                    "timestamp_created": 200,
                },
                {
                    "recommendationid": "r3",
                    "appid": 10,
                    "game_name": "Paid A",
                    "voted_up": True,
                    "playtime_hours": None,
                    "playtime_bucket": "2-10h",
                    "playtime_at_review": None,
                    "playtime_forever": None,
                    "is_free": False,
                    "price": 100.0,
                    "genres": ["Action", "RPG", "Action"],
                    "categories": [
                        "Single-player",
                        "Achievements",
                        "Achievements",
                    ],
                    "steam_purchase": True,
                    "received_for_free": True,
                    "platforms": {
                        "windows": True,
                        "mac": True,
                        "linux": False,
                    },
                    "votes_up": 6,
                    "votes_funny": 2,
                    "weighted_vote_score": "1.00",
                    "timestamp_created": 300,
                },
                {
                    "recommendationid": "r4",
                    "appid": 20,
                    "game_name": "Paid B",
                    "voted_up": False,
                    "playtime_hours": 20.0,
                    "playtime_bucket": "10-50h",
                    "playtime_at_review": 1200,
                    "playtime_forever": 1800,
                    "is_free": False,
                    "price": 300.0,
                    "genres": ["Strategy"],
                    "categories": ["Multi-player"],
                    "steam_purchase": False,
                    "received_for_free": False,
                    "platforms": {
                        "windows": True,
                        "mac": False,
                        "linux": False,
                    },
                    "votes_up": 0,
                    "votes_funny": 0,
                    "weighted_vote_score": "0.50",
                    "timestamp_created": None,
                },
                {
                    "recommendationid": "r5",
                    "appid": 30,
                    "game_name": "Free C",
                    "voted_up": True,
                    "playtime_hours": 60.0,
                    "playtime_bucket": "50h+",
                    "playtime_at_review": 3600,
                    "playtime_forever": 6000,
                    "is_free": True,
                    "price": 0.0,
                    "genres": None,
                    "categories": None,
                    "steam_purchase": False,
                    "received_for_free": True,
                    "platforms": {
                        "windows": True,
                        "mac": False,
                        "linux": True,
                    },
                    "votes_up": 10,
                    "votes_funny": 4,
                    "weighted_vote_score": None,
                    "timestamp_created": 500,
                },
            ],
            GOLD_ANALYTICS_SCHEMA,
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.temp.cleanup()

    def test_recommendation_by_game(self):
        actual = rows_by(recommendation_by_game(self.gold), "appid")
        self.assertEqual(set(actual), {10, 20, 30})

        self.assertEqual(actual[10]["game_name"], "Paid A")
        self.assertEqual(actual[10]["review_count"], 3)
        self.assertEqual(actual[10]["positive_reviews"], 2)
        self.assertEqual(actual[10]["negative_reviews"], 1)
        self.assertAlmostEqual(actual[10]["recommendation_rate"], 2 / 3)

        self.assertEqual(actual[20]["review_count"], 1)
        self.assertEqual(actual[20]["positive_reviews"], 0)
        self.assertEqual(actual[20]["negative_reviews"], 1)
        self.assertAlmostEqual(actual[20]["recommendation_rate"], 0.0)

        self.assertEqual(actual[30]["review_count"], 1)
        self.assertEqual(actual[30]["positive_reviews"], 1)
        self.assertEqual(actual[30]["negative_reviews"], 0)
        self.assertAlmostEqual(actual[30]["recommendation_rate"], 1.0)

    def test_recommendation_by_genre(self):
        actual = rows_by(recommendation_by_genre(self.gold), "genre")
        self.assertEqual(set(actual), {"Action", "RPG", "Strategy", "UNKNOWN"})

        for genre in ("Action", "RPG"):
            self.assertEqual(actual[genre]["review_count"], 3)
            self.assertEqual(actual[genre]["positive_reviews"], 2)
            self.assertEqual(actual[genre]["negative_reviews"], 1)
            self.assertAlmostEqual(actual[genre]["recommendation_rate"], 2 / 3)

        self.assertEqual(actual["Strategy"]["review_count"], 1)
        self.assertEqual(actual["Strategy"]["positive_reviews"], 0)
        self.assertEqual(actual["Strategy"]["negative_reviews"], 1)
        self.assertAlmostEqual(actual["Strategy"]["recommendation_rate"], 0.0)
        self.assertEqual(actual["UNKNOWN"]["review_count"], 1)
        self.assertEqual(actual["UNKNOWN"]["positive_reviews"], 1)
        self.assertEqual(actual["UNKNOWN"]["negative_reviews"], 0)
        self.assertAlmostEqual(actual["UNKNOWN"]["recommendation_rate"], 1.0)

    def test_recommendation_by_playtime(self):
        actual = rows_by(
            recommendation_by_playtime(self.gold),
            "playtime_bucket",
        )
        self.assertEqual(set(actual), {"0-2h", "2-10h", "10-50h", "50h+"})

        self.assertEqual(actual["2-10h"]["review_count"], 2)
        self.assertEqual(actual["2-10h"]["positive_reviews"], 1)
        self.assertEqual(actual["2-10h"]["negative_reviews"], 1)
        self.assertAlmostEqual(actual["2-10h"]["avg_playtime_hours"], 3.0)
        self.assertAlmostEqual(actual["2-10h"]["recommendation_rate"], 0.5)

        expected = {
            "0-2h": (1, 1, 0, 1.0, 1.0),
            "10-50h": (1, 0, 1, 20.0, 0.0),
            "50h+": (1, 1, 0, 60.0, 1.0),
        }
        for bucket, values in expected.items():
            row = actual[bucket]
            self.assertEqual(row["review_count"], values[0])
            self.assertEqual(row["positive_reviews"], values[1])
            self.assertEqual(row["negative_reviews"], values[2])
            self.assertAlmostEqual(row["avg_playtime_hours"], values[3])
            self.assertAlmostEqual(row["recommendation_rate"], values[4])

    def test_free_vs_paid_uses_game_level_price_statistics(self):
        actual = rows_by(free_vs_paid(self.gold), "game_type")
        self.assertEqual(set(actual), {"FREE", "PAID"})

        paid = actual["PAID"]
        self.assertEqual(paid["game_count"], 2)
        self.assertEqual(paid["review_count"], 4)
        self.assertEqual(paid["playtime_observed_count"], 3)
        self.assertEqual(paid["positive_reviews"], 2)
        self.assertEqual(paid["negative_reviews"], 2)
        self.assertAlmostEqual(paid["avg_playtime_hours"], 8.0)
        self.assertAlmostEqual(paid["recommendation_rate"], 0.5)
        self.assertAlmostEqual(paid["min_price"], 100.0)
        self.assertAlmostEqual(paid["max_price"], 300.0)
        self.assertAlmostEqual(paid["avg_price"], 200.0)

        free = actual["FREE"]
        self.assertEqual(free["game_count"], 1)
        self.assertEqual(free["review_count"], 1)
        self.assertEqual(free["playtime_observed_count"], 1)
        self.assertEqual(free["positive_reviews"], 1)
        self.assertEqual(free["negative_reviews"], 0)
        self.assertAlmostEqual(free["avg_playtime_hours"], 60.0)
        self.assertAlmostEqual(free["recommendation_rate"], 1.0)
        self.assertAlmostEqual(free["min_price"], 0.0)
        self.assertAlmostEqual(free["max_price"], 0.0)
        self.assertAlmostEqual(free["avg_price"], 0.0)

    def test_recommendation_by_platform(self):
        actual = rows_by(recommendation_by_platform(self.gold), "platform")
        self.assertEqual(set(actual), {"WINDOWS", "MAC", "LINUX"})

        expected = {
            "WINDOWS": (5, 3, 2, 3 / 5),
            "MAC": (3, 2, 1, 2 / 3),
            "LINUX": (1, 1, 0, 1.0),
        }
        for platform, values in expected.items():
            row = actual[platform]
            self.assertEqual(row["review_count"], values[0])
            self.assertEqual(row["positive_reviews"], values[1])
            self.assertEqual(row["negative_reviews"], values[2])
            self.assertAlmostEqual(row["recommendation_rate"], values[3])

    def test_recommendation_by_category(self):
        actual = rows_by(recommendation_by_category(self.gold), "category")
        self.assertEqual(
            set(actual),
            {"Single-player", "Achievements", "Multi-player", "UNKNOWN"},
        )

        for category in ("Single-player", "Achievements"):
            self.assertEqual(actual[category]["review_count"], 3)
            self.assertEqual(actual[category]["positive_reviews"], 2)
            self.assertEqual(actual[category]["negative_reviews"], 1)
            self.assertAlmostEqual(
                actual[category]["recommendation_rate"],
                2 / 3,
            )

        self.assertEqual(actual["Multi-player"]["review_count"], 1)
        self.assertEqual(actual["Multi-player"]["positive_reviews"], 0)
        self.assertEqual(actual["Multi-player"]["negative_reviews"], 1)
        self.assertAlmostEqual(
            actual["Multi-player"]["recommendation_rate"],
            0.0,
        )
        self.assertEqual(actual["UNKNOWN"]["review_count"], 1)
        self.assertEqual(actual["UNKNOWN"]["positive_reviews"], 1)
        self.assertEqual(actual["UNKNOWN"]["negative_reviews"], 0)
        self.assertAlmostEqual(actual["UNKNOWN"]["recommendation_rate"], 1.0)

    def test_recommendation_by_purchase_source(self):
        actual = rows_by(
            recommendation_by_purchase_source(self.gold),
            "purchase_source",
        )
        self.assertEqual(set(actual), {"STEAM_PURCHASE", "OTHER_SOURCE"})

        steam = actual["STEAM_PURCHASE"]
        self.assertEqual(steam["review_count"], 2)
        self.assertEqual(steam["positive_reviews"], 2)
        self.assertEqual(steam["negative_reviews"], 0)
        self.assertAlmostEqual(steam["avg_playtime_hours"], 1.0)
        self.assertAlmostEqual(steam["recommendation_rate"], 1.0)

        other = actual["OTHER_SOURCE"]
        self.assertEqual(other["review_count"], 3)
        self.assertEqual(other["positive_reviews"], 1)
        self.assertEqual(other["negative_reviews"], 2)
        self.assertAlmostEqual(other["avg_playtime_hours"], 83 / 3)
        self.assertAlmostEqual(other["recommendation_rate"], 1 / 3)
        self.assertEqual(
            sum(row["review_count"] for row in actual.values()),
            self.gold.count(),
        )

    def test_engagement_by_game(self):
        actual = rows_by(engagement_by_game(self.gold), "appid")
        paid_a = actual[10]
        self.assertEqual(paid_a["review_count"], 3)
        self.assertEqual(paid_a["playtime_at_review_observed_count"], 2)
        self.assertEqual(paid_a["playtime_forever_observed_count"], 2)
        self.assertAlmostEqual(paid_a["avg_playtime_at_review_hours"], 2.0)
        self.assertAlmostEqual(paid_a["avg_playtime_forever_hours"], 3.0)
        self.assertAlmostEqual(paid_a["avg_votes_up"], 4.0)
        self.assertAlmostEqual(paid_a["avg_votes_funny"], 2.0)
        self.assertAlmostEqual(paid_a["avg_weighted_vote_score"], 2 / 3)
        self.assertEqual(paid_a["positive_reviews"], 2)
        self.assertAlmostEqual(paid_a["recommendation_rate"], 2 / 3)

        paid_b = actual[20]
        self.assertAlmostEqual(paid_b["avg_playtime_at_review_hours"], 20.0)
        self.assertAlmostEqual(paid_b["avg_playtime_forever_hours"], 30.0)
        self.assertAlmostEqual(paid_b["avg_weighted_vote_score"], 0.5)

        free_c = actual[30]
        self.assertEqual(free_c["review_count"], 1)
        self.assertIsNone(free_c["avg_weighted_vote_score"])
        self.assertAlmostEqual(free_c["recommendation_rate"], 1.0)

    def test_label_profile(self):
        profile = label_profile(self.gold).first().asDict(recursive=True)
        self.assertEqual(profile["total_rows"], 5)
        self.assertEqual(profile["unique_recommendationid"], 5)
        self.assertEqual(profile["distinct_appids"], 3)
        self.assertEqual(profile["positive_count"], 3)
        self.assertEqual(profile["negative_count"], 2)
        self.assertEqual(profile["free_review_count"], 1)
        self.assertEqual(profile["paid_review_count"], 4)
        self.assertEqual(profile["null_playtime_at_review"], 1)
        self.assertEqual(profile["null_playtime_forever"], 1)
        self.assertEqual(profile["null_price"], 0)
        self.assertEqual(profile["null_genres"], 1)
        self.assertEqual(profile["null_timestamp_created"], 1)
        self.assertEqual(profile["null_weighted_vote_score"], 1)
        self.assertAlmostEqual(profile["positive_percentage"], 3 / 5)
        self.assertAlmostEqual(profile["negative_percentage"], 2 / 5)
        self.assertEqual(
            profile["positive_count"] + profile["negative_count"],
            profile["total_rows"],
        )


if __name__ == "__main__":
    unittest.main()
