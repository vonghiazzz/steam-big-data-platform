"""Read and validate the canonical Steam Bronze JSONL data with PySpark."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.config import HDFS_BRONZE_GAMES, HDFS_BRONZE_REVIEWS
from schemas.steam_bronze_schema import (
    STEAM_GAMES_BRONZE_SCHEMA,
    STEAM_REVIEWS_BRONZE_SCHEMA,
)


EXPECTED_GAMES = 50
EXPECTED_REVIEWS = 25_000
EXPECTED_VOTED_UP = {True: 18_321, False: 6_679}


def _record_check(failures: list[str], condition: bool, message: str) -> None:
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {message}")
    if not condition:
        failures.append(message)


def main() -> None:
    spark = SparkSession.builder.appName("Steam Bronze Ingestion Validation").getOrCreate()

    try:
        games_df = spark.read.schema(STEAM_GAMES_BRONZE_SCHEMA).json(
            HDFS_BRONZE_GAMES
        )
        reviews_df = spark.read.schema(STEAM_REVIEWS_BRONZE_SCHEMA).json(
            HDFS_BRONZE_REVIEWS
        )

        print("=== GAMES SCHEMA ===")
        games_df.printSchema()
        print("=== REVIEWS SCHEMA ===")
        reviews_df.printSchema()

        games_count = games_df.count()
        reviews_count = reviews_df.count()
        populated_game_data = games_df.filter(
            F.col("data").isNotNull() & F.col("data.name").isNotNull()
        ).count()
        populated_recommendation_ids = reviews_df.filter(
            F.col("review.recommendationid").isNotNull()
            & (F.length(F.col("review.recommendationid")) > 0)
        ).count()
        vote_distribution = {
            row["voted_up"]: row["count"]
            for row in reviews_df.groupBy("review.voted_up").count().collect()
        }

        print(f"Games count: {games_count}")
        print(f"Reviews count: {reviews_count}")
        print(f"review.voted_up distribution: {vote_distribution}")

        failures: list[str] = []
        _record_check(failures, games_count == EXPECTED_GAMES, "games count is 50")
        _record_check(
            failures,
            populated_game_data == games_count,
            "every game has populated data.name",
        )
        _record_check(
            failures, reviews_count == EXPECTED_REVIEWS, "reviews count is 25000"
        )
        _record_check(
            failures,
            populated_recommendation_ids == reviews_count,
            "every review has populated review.recommendationid",
        )
        _record_check(
            failures,
            vote_distribution == EXPECTED_VOTED_UP,
            "review.voted_up distribution is {True: 18321, False: 6679}",
        )

        if failures:
            raise RuntimeError("Bronze validation failed: " + "; ".join(failures))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
