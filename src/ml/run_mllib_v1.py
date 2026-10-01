"""Train and evaluate the fixed-baseline Steam Spark MLlib V1 models."""

from __future__ import annotations

import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.ml.evaluation import (
    evaluate_binary_predictions,
    majority_baseline_predictions,
)
from src.ml.feature_engineering import (
    FORBIDDEN_FEATURE_COLUMNS,
    SCALAR_FEATURE_COLUMNS,
    DEFAULT_SPLIT_SEED,
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


DEFAULT_INPUT_PATH = "/steam/gold/base"
DEFAULT_MODEL_ROOT = "/steam/models/mllib/v1"
DEFAULT_PREDICTIONS_PATH = "/steam/ml/mllib/v1/test_predictions"
DEFAULT_EVIDENCE_DIR = PROJECT_ROOT / "evidence/ml"


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _label_counts(df) -> dict[str, int]:
    return {
        str(int(float(row["label"]))): int(row["count"])
        for row in df.groupBy("label").count().orderBy("label").collect()
    }


def _format_profile(profile, *, run_id: str, input_path: str) -> str:
    lines = [
        f"run_id={run_id}",
        f"input_path={input_path}",
        f"dataset_fingerprint={profile.dataset_fingerprint}",
        f"rows={profile.rows}",
        f"unique_recommendationid={profile.unique_recommendation_ids}",
        f"null_recommendationid={profile.null_recommendation_ids}",
        f"distinct_appids={profile.distinct_appids}",
        f"positive={profile.positive}",
        f"negative={profile.negative}",
        f"invalid_voted_up={profile.invalid_target}",
        "candidate_feature_null_counts:",
    ]
    lines.extend(f"  {name}={value}" for name, value in profile.null_counts.items())
    return "\n".join(lines)


def _save_metrics(evidence_dir: Path, metrics, *, run_id: str) -> None:
    rows = [{"run_id": run_id, **metric.as_dict()} for metric in metrics]
    _write(evidence_dir / "model_metrics.json", json.dumps(rows, indent=2))
    with (evidence_dir / "model_metrics.csv").open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(rows[0]),
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    _write(
        evidence_dir / "confusion_matrices.txt",
        f"run_id={run_id}\n"
        + "\n".join(
            f"{row['model']}: TP={row['tp']} FP={row['fp']} "
            f"TN={row['tn']} FN={row['fn']}"
            for row in rows
        ),
    )


def _top_feature_lines(model_name: str, feature_names: list[str], values) -> list[str]:
    ranked = sorted(
        zip(feature_names, (float(value) for value in values)),
        key=lambda pair: (-abs(pair[1]), pair[0]),
    )[:25]
    return [f"{model_name}\t{name}\t{value:.10f}" for name, value in ranked]


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env", override=False)
    started = time.monotonic()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    input_path = os.getenv("ML_INPUT_PATH", DEFAULT_INPUT_PATH)
    model_root = os.getenv("ML_MODEL_ROOT", DEFAULT_MODEL_ROOT).rstrip("/")
    predictions_path = os.getenv(
        "ML_PREDICTIONS_PATH", DEFAULT_PREDICTIONS_PATH
    )
    evidence_dir = Path(os.getenv("ML_EVIDENCE_DIR", str(DEFAULT_EVIDENCE_DIR)))
    evidence_dir.mkdir(parents=True, exist_ok=True)
    builder = (
        SparkSession.builder.appName("Steam Spark MLlib V1")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv("ML_SHUFFLE_PARTITIONS", "4"),
        )
    )
    hdfs_default_fs = os.getenv("HDFS_DEFAULT_FS")
    if hdfs_default_fs:
        builder = builder.config("spark.hadoop.fs.defaultFS", hdfs_default_fs)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    try:
        source = spark.read.parquet(input_path).cache()
        profile = profile_source(source)
        _write(
            evidence_dir / "dataset_profile.txt",
            _format_profile(profile, run_id=run_id, input_path=input_path),
        )
        try:
            validate_source_profile(profile)
        except RuntimeError as exc:
            _write(
                evidence_dir / "training_summary.txt",
                "status=BLOCKED_SOURCE_CONTRACT\n"
                f"run_id={run_id}\n"
                f"spark_version={spark.version}\n"
                f"input_path={input_path}\n"
                f"dataset_fingerprint={profile.dataset_fingerprint}\n"
                f"error={exc}",
            )
            raise

        assert_leakage_guard()
        prepared = prepare_ml_rows(source).cache()
        if prepared.count() != profile.rows:
            raise RuntimeError("feature preparation changed the one-review-per-row grain")
        train, test = deterministic_stratified_split(
            prepared,
            seed=DEFAULT_SPLIT_SEED,
        )
        train = train.cache()
        test = test.cache()
        train_count = train.count()
        test_count = test.count()
        overlap = train.select("recommendationid").join(
            test.select("recommendationid"), "recommendationid", "inner"
        ).count()
        if train_count + test_count != profile.rows or overlap:
            raise RuntimeError(
                f"invalid split: train={train_count}, test={test_count}, overlap={overlap}"
            )
        train_labels = _label_counts(train)
        test_labels = _label_counts(test)
        if set(train_labels) != {"0", "1"} or set(test_labels) != {"0", "1"}:
            raise RuntimeError("both labels must appear in train and test")

        weights = training_class_weights(train)
        weighted_train = apply_class_weights(train, weights)
        weighted_test = apply_class_weights(test, weights)
        majority_label, baseline_predictions = majority_baseline_predictions(
            train, test
        )
        baseline_metrics = evaluate_binary_predictions(
            baseline_predictions,
            model_name="majority_baseline",
            score_column=None,
        )

        classifiers = {
            "logistic_regression": LogisticRegression(
                featuresCol="features",
                labelCol="label",
                weightCol="class_weight",
                maxIter=50,
                regParam=0.01,
                elasticNetParam=0.0,
                standardization=True,
                family="binomial",
            ),
            "random_forest": RandomForestClassifier(
                featuresCol="features",
                labelCol="label",
                weightCol="class_weight",
                numTrees=80,
                maxDepth=8,
                minInstancesPerNode=2,
                featureSubsetStrategy="sqrt",
                seed=501,
            ),
        }
        metrics = [baseline_metrics]
        prediction_outputs = []
        importance_lines = ["model\tfeature\tvalue"]
        feature_names = None
        parameter_lines = []
        for name, classifier in classifiers.items():
            pipeline = build_training_pipeline(classifier)
            model = pipeline.fit(weighted_train)
            names = feature_names_from_pipeline_model(model)
            if feature_names is None:
                feature_names = names
            elif names != feature_names:
                raise RuntimeError("model pipelines produced different feature ordering")
            predictions = model.transform(weighted_test).cache()
            metrics.append(
                evaluate_binary_predictions(predictions, model_name=name)
            )
            model_path = f"{model_root}/{name}"
            model.write().overwrite().save(model_path)
            prediction_outputs.append(
                predictions.select(
                    "recommendationid",
                    "label",
                    "prediction",
                    vector_to_array("probability")[1].alias("probability"),
                ).withColumn("model", F.lit(name))
            )
            classifier_model = model.stages[-1]
            if name == "logistic_regression":
                values = classifier_model.coefficients.toArray()
                importance_lines.extend(_top_feature_lines(name, names, values))
                parameter_lines.append(
                    f"{name}: intercept={classifier_model.intercept}; "
                    f"coefficient_count={len(values)}; {classifier.extractParamMap()}"
                )
            else:
                values = classifier_model.featureImportances.toArray()
                importance_lines.extend(_top_feature_lines(name, names, values))
                parameter_lines.append(f"{name}: {classifier.extractParamMap()}")
            predictions.unpersist()

        combined_predictions = prediction_outputs[0].unionByName(
            prediction_outputs[1]
        )
        combined_predictions.write.mode("overwrite").parquet(predictions_path)

        split_text = (
            f"run_id={run_id}\n"
            f"dataset_fingerprint={profile.dataset_fingerprint}\n"
            f"split_seed={DEFAULT_SPLIT_SEED}\n"
            f"train_count={train_count}\n"
            f"test_count={test_count}\n"
            f"overlap={overlap}\n"
            f"train_labels={json.dumps(train_labels, sort_keys=True)}\n"
            f"test_labels={json.dumps(test_labels, sort_keys=True)}\n"
            f"class_weights={json.dumps(weights, sort_keys=True)}\n"
            f"majority_label={majority_label}"
        )
        _write(evidence_dir / "split_profile.txt", split_text)
        _write(
            evidence_dir / "feature_contract.txt",
            "primary_scalar_features:\n  "
            + "\n  ".join(SCALAR_FEATURE_COLUMNS)
            + "\nvector_features:\n  genres (CountVectorizer binary=true)"
            + "\n  categories (CountVectorizer binary=true)"
            + "\nexcluded_features:\n  "
            + "\n  ".join(sorted(FORBIDDEN_FEATURE_COLUMNS))
            + "\nnotes:\n  playtime_forever excluded because it may be observed after review creation"
            + "\n  price is crawl-time metadata, not guaranteed historical review-time price"
            + "\n  nullable numeric and boolean-derived values use median imputation"
            + " fitted on training data only",
        )
        _save_metrics(evidence_dir, metrics, run_id=run_id)
        _write(evidence_dir / "feature_importance.txt", "\n".join(importance_lines))
        sample_rows = combined_predictions.orderBy("model", "recommendationid").limit(30).collect()
        sample_lines = ["model\trecommendationid\tlabel\tprediction\tprobability"]
        sample_lines.extend(
            f"{row.model}\t{row.recommendationid}\t{row.label}\t"
            f"{row.prediction}\t{row.probability}"
            for row in sample_rows
        )
        _write(evidence_dir / "predictions_sample.tsv", "\n".join(sample_lines))
        _write(
            evidence_dir / "training_summary.txt",
            "status=PASS\n"
            f"run_id={run_id}\n"
            f"spark_version={spark.version}\n"
            f"input_path={input_path}\n"
            f"dataset_fingerprint={profile.dataset_fingerprint}\n"
            f"game_count={profile.distinct_appids}\n"
            f"source_rows={profile.rows}\n"
            f"unique_recommendationid={profile.unique_recommendation_ids}\n"
            f"positive={profile.positive}\n"
            f"negative={profile.negative}\n"
            f"split_seed={DEFAULT_SPLIT_SEED}\n"
            f"train_count={train_count}\n"
            f"test_count={test_count}\n"
            f"feature_vector_dimension={len(feature_names or [])}\n"
            f"model_root={model_root}\n"
            f"logistic_regression_model_path={model_root}/logistic_regression\n"
            f"random_forest_model_path={model_root}/random_forest\n"
            f"predictions_path={predictions_path}\n"
            f"runtime_seconds={time.monotonic() - started:.3f}\n"
            + "\n".join(parameter_lines),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
