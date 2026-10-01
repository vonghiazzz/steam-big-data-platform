"""Feature preparation and deterministic splitting for MLlib V1."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer, Imputer, VectorAssembler
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


DEFAULT_SPLIT_SEED = 501

REQUIRED_SCHEMA = {
    "recommendationid": "string",
    "appid": "int",
    "game_name": "string",
    "voted_up": "boolean",
    "playtime_at_review": "int",
    "playtime_forever": "int",
    "steam_purchase": "boolean",
    "received_for_free": "boolean",
    "is_free": "boolean",
    "price": "double",
    "genres": "array<string>",
    "categories": "array<string>",
    "platforms": "struct<windows:boolean,mac:boolean,linux:boolean>",
    "recommendation_label": "int",
    "votes_up": "int",
    "votes_funny": "int",
    "weighted_vote_score": "string",
}

CANDIDATE_FEATURE_COLUMNS = (
    "playtime_at_review",
    "steam_purchase",
    "received_for_free",
    "is_free",
    "price",
    "genres",
    "categories",
    "platforms",
)

FORBIDDEN_FEATURE_COLUMNS = frozenset(
    {
        "voted_up",
        "label",
        "recommendation_label",
        "appid",
        "game_name",
        "recommendationid",
        "overall_positive_rate",
        "recommendation_rate",
        "positive_reviews",
        "negative_reviews",
        "votes_up",
        "votes_funny",
        "weighted_vote_score",
        "playtime_forever",
    }
)

IMPUTER_INPUT_COLUMNS = (
    "log1p_playtime_at_review",
    "steam_purchase_numeric",
    "received_for_free_numeric",
    "is_free_numeric",
    "log1p_price",
    "platform_windows_numeric",
    "platform_mac_numeric",
    "platform_linux_numeric",
)
IMPUTER_OUTPUT_COLUMNS = tuple(
    f"{column}_imputed" for column in IMPUTER_INPUT_COLUMNS
)
MISSING_INDICATOR_COLUMNS = tuple(
    f"{column}_missing" for column in IMPUTER_INPUT_COLUMNS
)
SCALAR_FEATURE_COLUMNS = IMPUTER_OUTPUT_COLUMNS + MISSING_INDICATOR_COLUMNS


@dataclass(frozen=True)
class SourceProfile:
    rows: int
    unique_recommendation_ids: int
    null_recommendation_ids: int
    positive: int
    negative: int
    invalid_target: int
    distinct_appids: int
    dataset_fingerprint: str
    null_counts: dict[str, int]


def assert_required_schema(df: DataFrame) -> None:
    actual = {field.name: field.dataType.simpleString() for field in df.schema}
    failures = []
    for column, expected_type in REQUIRED_SCHEMA.items():
        if column not in actual:
            failures.append(f"missing required column {column}")
        elif actual[column] != expected_type:
            failures.append(
                f"{column} has type {actual[column]}, expected {expected_type}"
            )
    if failures:
        raise RuntimeError("Gold Base schema is incompatible: " + "; ".join(failures))


def profile_source(df: DataFrame) -> SourceProfile:
    assert_required_schema(df)
    fingerprint_columns = [F.col(column) for column in REQUIRED_SCHEMA]
    profiled = df.withColumn(
        "_dataset_row_hash",
        F.xxhash64(*fingerprint_columns),
    )
    aggregate = profiled.agg(
        F.count("*").alias("rows"),
        F.countDistinct("recommendationid").alias("unique_ids"),
        F.sum(
            F.when(F.col("recommendationid").isNull(), 1).otherwise(0)
        ).alias("null_ids"),
        F.sum(F.when(F.col("voted_up") == F.lit(True), 1).otherwise(0)).alias(
            "positive"
        ),
        F.sum(F.when(F.col("voted_up") == F.lit(False), 1).otherwise(0)).alias(
            "negative"
        ),
        F.sum(F.when(F.col("voted_up").isNull(), 1).otherwise(0)).alias(
            "invalid_target"
        ),
        F.countDistinct("appid").alias("distinct_appids"),
        F.sum(F.col("_dataset_row_hash").cast("decimal(38,0)")).alias(
            "hash_sum"
        ),
        F.min("_dataset_row_hash").alias("hash_min"),
        F.max("_dataset_row_hash").alias("hash_max"),
        *[
            F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(
                f"null__{column}"
            )
            for column in CANDIDATE_FEATURE_COLUMNS
        ],
    ).first()
    values = aggregate.asDict()
    fingerprint_material = "|".join(
        str(values[name])
        for name in ("rows", "unique_ids", "hash_sum", "hash_min", "hash_max")
    )
    return SourceProfile(
        rows=int(values["rows"]),
        unique_recommendation_ids=int(values["unique_ids"]),
        null_recommendation_ids=int(values["null_ids"] or 0),
        positive=int(values["positive"] or 0),
        negative=int(values["negative"] or 0),
        invalid_target=int(values["invalid_target"] or 0),
        distinct_appids=int(values["distinct_appids"] or 0),
        dataset_fingerprint=hashlib.sha256(
            fingerprint_material.encode("utf-8")
        ).hexdigest(),
        null_counts={
            column: int(values[f"null__{column}"] or 0)
            for column in CANDIDATE_FEATURE_COLUMNS
        },
    )


def validate_source_profile(profile: SourceProfile) -> None:
    failures = []
    if profile.rows <= 0:
        failures.append("dataset is empty")
    if profile.null_recommendation_ids:
        failures.append(
            "recommendationid contains nulls: "
            f"{profile.null_recommendation_ids}"
        )
    if profile.unique_recommendation_ids != profile.rows:
        failures.append(
            "recommendationid is not unique: "
            f"rows={profile.rows}, unique={profile.unique_recommendation_ids}"
        )
    if profile.invalid_target:
        failures.append(f"voted_up contains nulls: {profile.invalid_target}")
    if profile.positive <= 0 or profile.negative <= 0:
        failures.append(
            "both target classes are required: "
            f"positive={profile.positive}, negative={profile.negative}"
        )
    if profile.positive + profile.negative != profile.rows:
        failures.append(
            "voted_up contains values outside the binary target contract: "
            f"rows={profile.rows}, positive={profile.positive}, "
            f"negative={profile.negative}"
        )
    if failures:
        raise RuntimeError(
            "MLlib V1 dynamic source integrity failed before training: "
            + "; ".join(failures)
        )


def _boolean_numeric(expression: Any):
    return (
        F.when(expression == F.lit(True), F.lit(1.0))
        .when(expression == F.lit(False), F.lit(0.0))
        .otherwise(F.lit(None).cast("double"))
    )


def _clean_tokens(column: str):
    empty = F.expr("cast(array() as array<string>)")
    return F.array_sort(
        F.array_distinct(
            F.filter(
                F.coalesce(F.col(column), empty),
                lambda item: item.isNotNull()
                & (F.length(F.trim(item)) > F.lit(0)),
            )
        )
    )


def prepare_ml_rows(df: DataFrame) -> DataFrame:
    """Create deterministic row-preserving features without fitting state."""
    assert_required_schema(df)
    playtime = F.when(
        F.col("playtime_at_review") >= F.lit(0),
        F.col("playtime_at_review").cast("double"),
    )
    price = F.when(
        F.col("price") >= F.lit(0),
        F.col("price").cast("double"),
    )
    numeric_expressions = {
        "log1p_playtime_at_review": F.log1p(playtime),
        "steam_purchase_numeric": _boolean_numeric(F.col("steam_purchase")),
        "received_for_free_numeric": _boolean_numeric(
            F.col("received_for_free")
        ),
        "is_free_numeric": _boolean_numeric(F.col("is_free")),
        "log1p_price": F.log1p(price),
        "platform_windows_numeric": _boolean_numeric(
            F.col("platforms.windows")
        ),
        "platform_mac_numeric": _boolean_numeric(F.col("platforms.mac")),
        "platform_linux_numeric": _boolean_numeric(
            F.col("platforms.linux")
        ),
    }
    prepared = df.select(
        "recommendationid",
        F.when(F.col("voted_up") == F.lit(True), F.lit(1.0))
        .when(F.col("voted_up") == F.lit(False), F.lit(0.0))
        .otherwise(F.lit(None).cast("double"))
        .alias("label"),
        *[expression.alias(name) for name, expression in numeric_expressions.items()],
        _clean_tokens("genres").alias("genres_tokens"),
        _clean_tokens("categories").alias("categories_tokens"),
    )
    return prepared.select(
        "*",
        *[
            F.col(column).isNull().cast("double").alias(f"{column}_missing")
            for column in IMPUTER_INPUT_COLUMNS
        ],
    )


def deterministic_stratified_split(
    df: DataFrame,
    *,
    train_fraction: float = 0.8,
    seed: int = DEFAULT_SPLIT_SEED,
) -> tuple[DataFrame, DataFrame]:
    if not 0.0 < train_fraction < 1.0:
        raise ValueError("train_fraction must be between zero and one")
    ordering = Window.partitionBy("label").orderBy(
        F.xxhash64("recommendationid", F.lit(str(seed))),
        F.col("recommendationid"),
    )
    label_partition = Window.partitionBy("label")
    ranked = (
        df.withColumn("_split_rank", F.row_number().over(ordering))
        .withColumn("_label_count", F.count("*").over(label_partition))
        .withColumn(
            "_train_count",
            F.floor(F.col("_label_count") * F.lit(train_fraction)).cast("long"),
        )
    )
    internal = ("_split_rank", "_label_count", "_train_count")
    train = ranked.filter(F.col("_split_rank") <= F.col("_train_count")).drop(
        *internal
    )
    test = ranked.filter(F.col("_split_rank") > F.col("_train_count")).drop(
        *internal
    )
    return train, test


def training_class_weights(train: DataFrame) -> dict[float, float]:
    counts = {float(row["label"]): int(row["count"]) for row in train.groupBy("label").count().collect()}
    if set(counts) != {0.0, 1.0} or min(counts.values()) <= 0:
        raise RuntimeError(f"training split must contain both labels: {counts}")
    total = sum(counts.values())
    return {label: total / (2.0 * count) for label, count in counts.items()}


def apply_class_weights(
    df: DataFrame,
    weights: dict[float, float],
) -> DataFrame:
    return df.withColumn(
        "class_weight",
        F.when(F.col("label") == F.lit(1.0), F.lit(weights[1.0]))
        .when(F.col("label") == F.lit(0.0), F.lit(weights[0.0]))
        .otherwise(F.lit(None).cast("double")),
    )


def build_training_pipeline(classifier: Any) -> Pipeline:
    imputer = Imputer(
        strategy="median",
        inputCols=list(IMPUTER_INPUT_COLUMNS),
        outputCols=list(IMPUTER_OUTPUT_COLUMNS),
    )
    genres = CountVectorizer(
        inputCol="genres_tokens",
        outputCol="genres_features",
        binary=True,
        minDF=1.0,
    )
    categories = CountVectorizer(
        inputCol="categories_tokens",
        outputCol="categories_features",
        binary=True,
        minDF=1.0,
    )
    assembler = VectorAssembler(
        inputCols=list(SCALAR_FEATURE_COLUMNS)
        + ["genres_features", "categories_features"],
        outputCol="features",
        handleInvalid="error",
    )
    return Pipeline(stages=[imputer, genres, categories, assembler, classifier])


def feature_names_from_pipeline_model(model: Any) -> list[str]:
    genres_model = model.stages[1]
    categories_model = model.stages[2]
    names = list(SCALAR_FEATURE_COLUMNS)
    names.extend(f"genre={value}" for value in genres_model.vocabulary)
    names.extend(f"category={value}" for value in categories_model.vocabulary)
    return names


def assert_leakage_guard() -> None:
    declared = set(CANDIDATE_FEATURE_COLUMNS)
    overlap = declared & FORBIDDEN_FEATURE_COLUMNS
    if overlap:
        raise RuntimeError(f"forbidden leakage features configured: {sorted(overlap)}")
