import unittest
from decimal import Decimal

from src.serving.mongodb_loader import (
    COLLECTION_SPECS,
    MISSING_PLAYTIME_ID,
    SERVING_SNAPSHOT,
    build_documents,
    natural_id,
    to_python_native,
    validate_database,
)


class FakeRow:
    def __init__(self, values):
        self.values = values

    def asDict(self, recursive=False):
        return dict(self.values)


class FakeCollection:
    def __init__(self, documents):
        self.documents = documents

    def find(self, query, projection=None):
        return list(self.documents)

    def find_one(self, query):
        return next(
            (
                document
                for document in self.documents
                if all(document.get(key) == value for key, value in query.items())
            ),
            None,
        )


class FakeDatabase:
    def __init__(self, collections):
        self.collections = {
            name: FakeCollection(documents)
            for name, documents in collections.items()
        }

    def __getitem__(self, name):
        return self.collections[name]


class MongoDBServingV1Test(unittest.TestCase):
    def test_collection_contract_and_expected_counts(self):
        self.assertEqual(
            list(COLLECTION_SPECS),
            [
                "game_metrics",
                "genre_metrics",
                "playtime_metrics",
                "free_paid_metrics",
                "engagement_metrics",
                "label_profile",
                "platform_metrics",
                "category_metrics",
                "purchase_metrics",
            ],
        )
        self.assertEqual(
            [spec.expected_count for spec in COLLECTION_SPECS.values()],
            [50, 17, 5, 2, 50, 1, 3, 59, 2],
        )

    def test_natural_ids_are_deterministic(self):
        cases = {
            "game_metrics": ({"appid": 730}, 730),
            "engagement_metrics": ({"appid": 570}, 570),
            "genre_metrics": ({"genre": "Action"}, "Action"),
            "free_paid_metrics": ({"game_type": "FREE"}, "FREE"),
            "platform_metrics": ({"platform": "WINDOWS"}, "WINDOWS"),
            "category_metrics": ({"category": "UNKNOWN"}, "UNKNOWN"),
            "purchase_metrics": (
                {"purchase_source": "STEAM_PURCHASE"},
                "STEAM_PURCHASE",
            ),
            "label_profile": ({"total_rows": 25_000}, "historical_baseline"),
        }
        for collection_name, (document, expected) in cases.items():
            with self.subTest(collection_name=collection_name):
                self.assertEqual(natural_id(collection_name, document), expected)

    def test_missing_playtime_identity_preserves_analytical_null(self):
        documents = build_documents(
            "playtime_metrics",
            [
                FakeRow(
                    {
                        "playtime_bucket": None,
                        "review_count": 100,
                        "recommendation_rate": 0.5,
                    }
                )
            ],
        )
        self.assertEqual(documents[0]["_id"], MISSING_PLAYTIME_ID)
        self.assertIsNone(documents[0]["playtime_bucket"])

    def test_row_conversion_preserves_numeric_types(self):
        documents = build_documents(
            "game_metrics",
            [
                FakeRow(
                    {
                        "appid": 730,
                        "game_name": "Counter-Strike 2",
                        "review_count": 500,
                        "positive_reviews": 330,
                        "negative_reviews": 170,
                        "recommendation_rate": 0.66,
                        "price": Decimal("10.50"),
                    }
                )
            ],
        )
        document = documents[0]
        self.assertIsInstance(document["appid"], int)
        self.assertIsInstance(document["review_count"], int)
        self.assertIsInstance(document["recommendation_rate"], float)
        self.assertIsInstance(document["price"], float)
        self.assertEqual(document["serving_snapshot"], SERVING_SNAPSHOT)

    def test_nested_values_convert_to_python_native_types(self):
        converted = to_python_native(
            FakeRow({"values": (Decimal("1.25"), Decimal("2.50"))})
        )
        self.assertEqual(converted, {"values": [1.25, 2.5]})

    def test_duplicate_natural_keys_are_rejected_before_writes(self):
        rows = [
            {"genre": "Action", "review_count": 10},
            {"genre": "Action", "review_count": 20},
        ]
        with self.assertRaisesRegex(ValueError, "duplicate natural key"):
            build_documents("genre_metrics", rows)

    def test_validate_database_accepts_source_derived_counts(self):
        profile = {
            "_id": "historical_baseline",
            "total_rows": 100,
            "unique_recommendationid": 100,
            "distinct_appids": 1,
            "positive_count": 60,
            "negative_count": 40,
        }
        documents = {
            "game_metrics": [
                {
                    "_id": 730,
                    "appid": 730,
                    "review_count": 100,
                    "positive_reviews": 60,
                    "negative_reviews": 40,
                    "recommendation_rate": 0.6,
                }
            ],
            "genre_metrics": [
                {
                    "_id": "Action",
                    "genre": "Action",
                    "review_count": 100,
                    "positive_reviews": 60,
                    "negative_reviews": 40,
                    "recommendation_rate": 0.6,
                }
            ],
            "playtime_metrics": [
                {
                    "_id": "0-10",
                    "playtime_bucket": "0-10",
                    "review_count": 100,
                    "positive_reviews": 60,
                    "negative_reviews": 40,
                    "recommendation_rate": 0.6,
                }
            ],
            "free_paid_metrics": [
                {
                    "_id": "FREE",
                    "game_type": "FREE",
                    "review_count": 100,
                    "positive_reviews": 60,
                    "negative_reviews": 40,
                    "recommendation_rate": 0.6,
                }
            ],
            "engagement_metrics": [
                {
                    "_id": 730,
                    "appid": 730,
                    "review_count": 100,
                    "positive_reviews": 60,
                    "recommendation_rate": 0.6,
                }
            ],
            "label_profile": [profile],
            "platform_metrics": [
                {
                    "_id": "WINDOWS",
                    "platform": "WINDOWS",
                    "review_count": 100,
                    "positive_reviews": 60,
                    "negative_reviews": 40,
                    "recommendation_rate": 0.6,
                }
            ],
            "category_metrics": [
                {
                    "_id": "Action",
                    "category": "Action",
                    "review_count": 100,
                    "positive_reviews": 60,
                    "negative_reviews": 40,
                    "recommendation_rate": 0.6,
                }
            ],
            "purchase_metrics": [
                {
                    "_id": "STEAM_PURCHASE",
                    "purchase_source": "STEAM_PURCHASE",
                    "review_count": 100,
                    "positive_reviews": 60,
                    "negative_reviews": 40,
                    "recommendation_rate": 0.6,
                }
            ],
        }
        database = FakeDatabase(documents)
        expected_counts = {name: 1 for name in COLLECTION_SPECS}

        counts, totals = validate_database(
            database,
            expected_counts=expected_counts,
            expected_reviews=100,
            expected_games=1,
        )

        self.assertEqual(counts, expected_counts)
        self.assertEqual(totals["reviews"], 100)
        self.assertEqual(totals["positive"], 60)
        self.assertEqual(totals["negative"], 40)

    def test_validate_database_rejects_inconsistent_source_counts(self):
        database = FakeDatabase({name: [] for name in COLLECTION_SPECS})
        expected_counts = {name: 1 for name in COLLECTION_SPECS}

        with self.assertRaisesRegex(RuntimeError, "game_metrics count 0 != 1"):
            validate_database(
                database,
                expected_counts=expected_counts,
                expected_reviews=100,
                expected_games=1,
            )


if __name__ == "__main__":
    unittest.main()
