"""Create and validate the one-review-per-row Steam Gold Base dataset."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def create_gold_dataset(
    reviews_df: DataFrame,
    games_df: DataFrame,
) -> DataFrame:
    """Join Silver datasets without exploding game collection fields."""
    reviews = reviews_df.alias("reviews")
    games = games_df.alias("games")
    playtime_hours = F.col("reviews.playtime_hours")

    playtime_bucket = (
        F.when(
            playtime_hours.isNull() | (playtime_hours < F.lit(0.0)),
            F.lit(None).cast("string"),
        )
        .when(playtime_hours < F.lit(2.0), F.lit("0-2h"))
        .when(playtime_hours < F.lit(10.0), F.lit("2-10h"))
        .when(playtime_hours < F.lit(50.0), F.lit("10-50h"))
        .otherwise(F.lit("50h+"))
    )

    return reviews.join(
        games,
        F.col("reviews.appid") == F.col("games.appid"),
        "inner",
    ).select(
        F.col("reviews.recommendationid").alias("recommendationid"),
        F.col("reviews.appid").alias("appid"),
        F.col("reviews.game_name").alias("game_name"),
        F.col("reviews.voted_up").alias("voted_up"),
        F.col("reviews.playtime_at_review").alias("playtime_at_review"),
        F.col("reviews.playtime_forever").alias("playtime_forever"),
        F.col("reviews.steam_purchase").alias("steam_purchase"),
        F.col("reviews.received_for_free").alias("received_for_free"),
        F.col("games.is_free").alias("is_free"),
        F.col("games.price").alias("price"),
        F.col("games.currency").alias("currency"),
        F.col("games.genres").alias("genres"),
        F.col("games.categories").alias("categories"),
        F.col("games.platforms").alias("platforms"),
        F.col("games.release_date").alias("release_date"),
        F.col("reviews.timestamp_created").alias("timestamp_created"),
        F.col("reviews.votes_up").alias("votes_up"),
        F.col("reviews.votes_funny").alias("votes_funny"),
        F.col("reviews.weighted_vote_score").alias("weighted_vote_score"),
        F.col("reviews.recommendation_label").alias("recommendation_label"),
        playtime_hours.alias("playtime_hours"),
        playtime_bucket.alias("playtime_bucket"),
    )


def _duplicate_key_count(df: DataFrame, key: str) -> int:
    return df.groupBy(key).count().filter(F.col("count") > 1).count()


def validate_gold_dataset(
    reviews_df: DataFrame,
    games_df: DataFrame,
    gold_df: DataFrame,
) -> None:
    """Enforce one Gold row per Silver review before Gold is written."""
    reviews_count = reviews_df.count()
    gold_count = gold_df.count()

    null_game_appid_count = games_df.filter(F.col("appid").isNull()).count()
    duplicate_game_appid_count = _duplicate_key_count(games_df, "appid")
    invalid_review_key_count = reviews_df.filter(
        F.col("recommendationid").isNull()
        | (F.length(F.trim(F.col("recommendationid"))) == 0)
    ).count()
    duplicate_review_key_count = _duplicate_key_count(
        reviews_df,
        "recommendationid",
    )
    unmatched_reviews_count = reviews_df.join(
        games_df.select("appid"),
        on="appid",
        how="left_anti",
    ).count()
    invalid_gold_key_count = gold_df.filter(
        F.col("recommendationid").isNull()
        | (F.length(F.trim(F.col("recommendationid"))) == 0)
    ).count()
    duplicate_gold_key_count = _duplicate_key_count(
        gold_df,
        "recommendationid",
    )

    print("=== GOLD BASE VALIDATION ===")
    print(f"Silver Reviews: {reviews_count}")
    print(f"Gold Base: {gold_count}")
    print(f"Null Games appid: {null_game_appid_count}")
    print(f"Duplicate Games appid keys: {duplicate_game_appid_count}")
    print(f"Invalid Silver Reviews keys: {invalid_review_key_count}")
    print(f"Duplicate Silver Reviews keys: {duplicate_review_key_count}")
    print(f"Unmatched Reviews: {unmatched_reviews_count}")
    print(f"Invalid Gold keys: {invalid_gold_key_count}")
    print(f"Duplicate Gold keys: {duplicate_gold_key_count}")

    failures = []
    if null_game_appid_count:
        failures.append(f"found {null_game_appid_count} null Games appid rows")
    if duplicate_game_appid_count:
        failures.append(
            f"found {duplicate_game_appid_count} duplicate Games appid keys"
        )
    if invalid_review_key_count:
        failures.append(
            f"found {invalid_review_key_count} invalid Silver Reviews keys"
        )
    if duplicate_review_key_count:
        failures.append(
            f"found {duplicate_review_key_count} duplicate Silver Reviews keys"
        )
    if unmatched_reviews_count:
        failures.append(f"found {unmatched_reviews_count} unmatched reviews")
    if gold_count != reviews_count:
        failures.append(
            f"Gold count {gold_count} does not equal Silver Reviews {reviews_count}"
        )
    if invalid_gold_key_count:
        failures.append(f"found {invalid_gold_key_count} invalid Gold keys")
    if duplicate_gold_key_count:
        failures.append(
            f"found {duplicate_gold_key_count} duplicate Gold recommendationid keys"
        )

    if failures:
        raise RuntimeError("Gold Base validation failed: " + "; ".join(failures))
