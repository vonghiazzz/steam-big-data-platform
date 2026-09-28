"""Build validated Steam Silver datasets from canonical HDFS Bronze JSONL."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.config import (
    HDFS_BRONZE_GAMES,
    HDFS_BRONZE_REVIEWS,
    HDFS_SILVER_GAMES,
    HDFS_SILVER_REVIEWS,
)
from schemas.steam_bronze_schema import (
    STEAM_GAMES_BRONZE_SCHEMA,
    STEAM_REVIEWS_BRONZE_SCHEMA,
)
from silver.clean_games import transform_games
from silver.clean_reviews import transform_reviews


def _duplicate_key_count(df: DataFrame, key: str) -> int:
    return df.groupBy(key).count().filter(F.col("count") > 1).count()


def _distribution(df: DataFrame, column: str) -> dict[object, int]:
    return {
        row["value"]: row["count"]
        for row in (
            df.select(F.col(column).alias("value"))
            .groupBy("value")
            .count()
            .collect()
        )
    }


def _raise_for_failures(dataset: str, failures: list[str]) -> None:
    if failures:
        raise RuntimeError(f"{dataset} validation failed: " + "; ".join(failures))


def validate_silver_games(
    bronze_games_df: DataFrame,
    silver_games_df: DataFrame,
) -> None:
    """Reject invalid game keys or any unexpected cardinality change."""
    input_count = bronze_games_df.count()
    output_count = silver_games_df.count()
    null_appid_count = silver_games_df.filter(F.col("appid").isNull()).count()
    duplicate_appid_count = _duplicate_key_count(silver_games_df, "appid")

    print("=== SILVER GAMES VALIDATION ===")
    print(f"Bronze Games: {input_count}")
    print(f"Silver Games: {output_count}")
    print(f"Null appid: {null_appid_count}")
    print(f"Duplicate appid keys: {duplicate_appid_count}")

    failures = []
    if output_count != input_count:
        failures.append(
            f"row count changed from {input_count} to {output_count}"
        )
    if null_appid_count:
        failures.append(f"found {null_appid_count} null appid rows")
    if duplicate_appid_count:
        failures.append(f"found {duplicate_appid_count} duplicate appid keys")

    _raise_for_failures("Silver Games", failures)


def validate_silver_reviews(
    bronze_reviews_df: DataFrame,
    silver_reviews_df: DataFrame,
) -> None:
    """Reject invalid review keys, duplicates, and label/cardinality changes."""
    input_count = bronze_reviews_df.count()
    output_count = silver_reviews_df.count()
    null_appid_count = silver_reviews_df.filter(F.col("appid").isNull()).count()
    invalid_recommendationid_count = silver_reviews_df.filter(
        F.col("recommendationid").isNull()
        | (F.length(F.trim(F.col("recommendationid"))) == 0)
    ).count()
    duplicate_recommendationid_count = _duplicate_key_count(
        silver_reviews_df,
        "recommendationid",
    )

    vote_distribution = _distribution(bronze_reviews_df, "review.voted_up")
    label_distribution = _distribution(
        silver_reviews_df,
        "recommendation_label",
    )
    expected_label_distribution = {
        1 if voted_up is True else 0 if voted_up is False else None: count
        for voted_up, count in vote_distribution.items()
    }

    expected_label = (
        F.when(F.col("voted_up") == F.lit(True), F.lit(1))
        .when(F.col("voted_up") == F.lit(False), F.lit(0))
        .otherwise(F.lit(None).cast("integer"))
    )
    label_mismatch_count = silver_reviews_df.filter(
        ~F.col("recommendation_label").eqNullSafe(expected_label)
    ).count()

    print("=== SILVER REVIEWS VALIDATION ===")
    print(f"Bronze Reviews: {input_count}")
    print(f"Silver Reviews: {output_count}")
    print(f"Null appid: {null_appid_count}")
    print(f"Null/empty recommendationid: {invalid_recommendationid_count}")
    print(f"Duplicate recommendationid keys: {duplicate_recommendationid_count}")
    print(f"voted_up distribution: {vote_distribution}")
    print(f"recommendation_label distribution: {label_distribution}")

    failures = []
    if output_count != input_count:
        failures.append(
            f"row count changed from {input_count} to {output_count}"
        )
    if null_appid_count:
        failures.append(f"found {null_appid_count} null appid rows")
    if invalid_recommendationid_count:
        failures.append(
            f"found {invalid_recommendationid_count} null/empty recommendationid rows"
        )
    if duplicate_recommendationid_count:
        failures.append(
            "found "
            f"{duplicate_recommendationid_count} duplicate recommendationid keys"
        )
    if label_distribution != expected_label_distribution or label_mismatch_count:
        failures.append(
            "recommendation_label does not preserve the voted_up distribution"
        )

    _raise_for_failures("Silver Reviews", failures)


def main() -> None:
    spark = SparkSession.builder.appName("Steam Silver Pipeline").getOrCreate()

    try:
        bronze_games_df = spark.read.schema(STEAM_GAMES_BRONZE_SCHEMA).json(
            HDFS_BRONZE_GAMES
        )
        bronze_reviews_df = spark.read.schema(STEAM_REVIEWS_BRONZE_SCHEMA).json(
            HDFS_BRONZE_REVIEWS
        )

        silver_games_df = transform_games(bronze_games_df)
        silver_reviews_df = transform_reviews(bronze_reviews_df)

        # Validate both outputs before writing either dataset. Duplicate keys are
        # rejected explicitly; no rows are silently filtered or deduplicated.
        validate_silver_games(bronze_games_df, silver_games_df)
        validate_silver_reviews(bronze_reviews_df, silver_reviews_df)

        silver_games_df.write.mode("overwrite").parquet(HDFS_SILVER_GAMES)
        silver_reviews_df.write.mode("overwrite").parquet(HDFS_SILVER_REVIEWS)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
