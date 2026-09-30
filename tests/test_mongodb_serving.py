import unittest
from decimal import Decimal

from src.serving.mongodb_loader import (
    COLLECTION_SPECS,
    MISSING_PLAYTIME_ID,
    SERVING_SNAPSHOT,
    build_documents,
    natural_id,
    to_python_native,
)


class FakeRow:
    def __init__(self, values):
        self.values = values

    def asDict(self, recursive=False):
        return dict(self.values)


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


if __name__ == "__main__":
    unittest.main()
