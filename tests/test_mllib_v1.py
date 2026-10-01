import math
import tempfile
import unittest

from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.sql import SparkSession

from src.ml.evaluation import evaluate_binary_predictions
from src.ml.feature_engineering import (
    FORBIDDEN_FEATURE_COLUMNS,
    SCALAR_FEATURE_COLUMNS,
    SourceProfile,
    apply_class_weights,
    assert_leakage_guard,
    build_training_pipeline,
    deterministic_stratified_split,
    feature_names_from_pipeline_model,
    prepare_ml_rows,
    profile_source,
    training_class_weights,
    validate_source_profile,
)


class MLlibV1Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("MLlib V1 unit tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.warehouse.dir", cls.temp.name)
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        rows = []
        for index in range(20):
            positive = index % 2 == 0
            rows.append(
                {
                    "recommendationid": f"r{index:02d}",
                    "appid": 10 + index % 3,
                    "game_name": f"Game {index % 3}",
                    "voted_up": positive,
                    "playtime_at_review": None if index == 3 else index * 30,
                    "playtime_forever": index * 60,
                    "steam_purchase": None if index == 4 else index % 3 != 0,
                    "received_for_free": index % 5 == 0,
                    "is_free": index % 4 == 0,
                    "price": None if index == 5 else float(index * 1000),
                    "genres": None if index == 6 else ["Action", "RPG" if positive else "Strategy"],
                    "categories": ["Single-player", "Achievements" if positive else "Multi-player"],
                    "platforms": {
                        "windows": True,
                        "mac": index % 2 == 0,
                        "linux": None if index == 7 else index % 3 == 0,
                    },
                    "recommendation_label": 1 if positive else 0,
                    "votes_up": index,
                    "votes_funny": 0,
                    "weighted_vote_score": "0.5",
                }
            )
        cls.gold = cls.spark.createDataFrame(
            rows,
            """
            recommendationid string, appid int, game_name string,
            voted_up boolean, playtime_at_review int, playtime_forever int,
            steam_purchase boolean, received_for_free boolean, is_free boolean,
            price double, genres array<string>, categories array<string>,
            platforms struct<windows:boolean,mac:boolean,linux:boolean>,
            recommendation_label int, votes_up int, votes_funny int,
            weighted_vote_score string
            """,
        )
        cls.prepared = prepare_ml_rows(cls.gold).cache()

    @classmethod
    def tearDownClass(cls):
        cls.prepared.unpersist()
        cls.spark.stop()
        cls.temp.cleanup()

    def test_label_mapping_and_row_grain_and_boolean_conversion(self):
        self.assertEqual(self.prepared.count(), self.gold.count())
        values = {
            row.recommendationid: row
            for row in self.prepared.select(
                "recommendationid",
                "label",
                "steam_purchase_numeric",
                "received_for_free_numeric",
            ).collect()
        }
        self.assertEqual(values["r00"].label, 1.0)
        self.assertEqual(values["r01"].label, 0.0)
        self.assertIsNone(values["r04"].steam_purchase_numeric)
        self.assertEqual(values["r05"].received_for_free_numeric, 1.0)

    def test_deterministic_stratified_split_has_no_overlap(self):
        train_a, test_a = deterministic_stratified_split(self.prepared)
        train_b, test_b = deterministic_stratified_split(self.prepared)
        train_ids_a = {row[0] for row in train_a.select("recommendationid").collect()}
        train_ids_b = {row[0] for row in train_b.select("recommendationid").collect()}
        test_ids_a = {row[0] for row in test_a.select("recommendationid").collect()}
        test_ids_b = {row[0] for row in test_b.select("recommendationid").collect()}
        self.assertEqual(train_ids_a, train_ids_b)
        self.assertEqual(test_ids_a, test_ids_b)
        self.assertFalse(train_ids_a & test_ids_a)
        self.assertEqual(len(train_ids_a) + len(test_ids_a), 20)
        self.assertEqual(train_a.groupBy("label").count().count(), 2)
        self.assertEqual(test_a.groupBy("label").count().count(), 2)

    def test_leakage_guard_and_feature_contract(self):
        assert_leakage_guard()
        self.assertFalse(set(SCALAR_FEATURE_COLUMNS) & FORBIDDEN_FEATURE_COLUMNS)
        self.assertIn("playtime_forever", FORBIDDEN_FEATURE_COLUMNS)
        self.assertIn("recommendation_label", FORBIDDEN_FEATURE_COLUMNS)
        self.assertIn("appid", FORBIDDEN_FEATURE_COLUMNS)

    def test_dynamic_source_contract_accepts_valid_growing_dataset(self):
        profile = profile_source(self.gold)
        validate_source_profile(profile)
        self.assertEqual(profile.rows, 20)
        self.assertEqual(profile.unique_recommendation_ids, 20)
        self.assertEqual(profile.null_recommendation_ids, 0)
        self.assertEqual(len(profile.dataset_fingerprint), 64)

    def test_dynamic_source_contract_rejects_integrity_failure(self):
        profile = SourceProfile(
            rows=40_000,
            unique_recommendation_ids=39_999,
            null_recommendation_ids=1,
            positive=31_398,
            negative=8_602,
            invalid_target=1,
            distinct_appids=80,
            dataset_fingerprint="fixture",
            null_counts={},
        )
        with self.assertRaisesRegex(RuntimeError, "dynamic source integrity"):
            validate_source_profile(profile)

    def _fit_model(self, classifier):
        train, test = deterministic_stratified_split(self.prepared)
        weights = training_class_weights(train)
        model = build_training_pipeline(classifier).fit(
            apply_class_weights(train, weights)
        )
        transformed = model.transform(apply_class_weights(test, weights))
        self.assertEqual(transformed.count(), test.count())
        for row in transformed.select("features").collect():
            self.assertFalse(any(math.isnan(float(value)) for value in row.features))
        names = feature_names_from_pipeline_model(model)
        self.assertEqual(len(names), transformed.first().features.size)
        return model, transformed, names

    def test_logistic_pipeline_fits_nulls_without_exploding_arrays(self):
        model, transformed, names = self._fit_model(
            LogisticRegression(
                featuresCol="features",
                labelCol="label",
                weightCol="class_weight",
                maxIter=5,
            )
        )
        self.assertEqual(names, feature_names_from_pipeline_model(model))
        self.assertEqual(
            transformed.select("recommendationid").distinct().count(),
            transformed.count(),
        )
        self.assertTrue(any(name.startswith("genre=") for name in names))
        self.assertTrue(any(name.startswith("category=") for name in names))
        second_model, _, second_names = self._fit_model(
            LogisticRegression(
                featuresCol="features",
                labelCol="label",
                weightCol="class_weight",
                maxIter=5,
            )
        )
        self.assertEqual(names, second_names)
        self.assertEqual(
            feature_names_from_pipeline_model(model),
            feature_names_from_pipeline_model(second_model),
        )

    def test_random_forest_pipeline_fits(self):
        self._fit_model(
            RandomForestClassifier(
                featuresCol="features",
                labelCol="label",
                weightCol="class_weight",
                numTrees=5,
                maxDepth=3,
                seed=501,
            )
        )

    def test_confusion_matrix_and_positive_class_metrics(self):
        predictions = self.spark.createDataFrame(
            [(1.0, 1.0), (1.0, 0.0), (0.0, 1.0), (0.0, 0.0)],
            "label double, prediction double",
        )
        metrics = evaluate_binary_predictions(
            predictions,
            model_name="fixture",
            score_column=None,
        )
        self.assertEqual((metrics.tp, metrics.fp, metrics.tn, metrics.fn), (1, 1, 1, 1))
        self.assertEqual(metrics.accuracy, 0.5)
        self.assertEqual(metrics.precision, 0.5)
        self.assertEqual(metrics.recall, 0.5)
        self.assertEqual(metrics.f1, 0.5)


if __name__ == "__main__":
    unittest.main()
