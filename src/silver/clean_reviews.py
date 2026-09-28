"""Flatten canonical Steam Bronze review records for the Silver layer."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_reviews(reviews_df: DataFrame) -> DataFrame:
    """Return one Silver row for every canonical Bronze review row.

    Raw analytical fields are retained alongside derived fields. No rows are
    filtered or deduplicated here; invalid and duplicate keys are rejected by
    the Silver pipeline before any output is written.
    """
    voted_up = F.col("review.voted_up")
    playtime_at_review = F.col("review.author.playtime_at_review")

    recommendation_label = (
        F.when(voted_up == F.lit(True), F.lit(1))
        .when(voted_up == F.lit(False), F.lit(0))
        .otherwise(F.lit(None).cast("integer"))
    )

    return reviews_df.select(
        F.col("review.recommendationid").alias("recommendationid"),
        F.col("appid").alias("appid"),
        F.col("game_name").alias("game_name"),
        F.col("review.language").alias("language"),
        voted_up.alias("voted_up"),
        playtime_at_review.alias("playtime_at_review"),
        F.col("review.author.playtime_forever").alias("playtime_forever"),
        F.col("review.steam_purchase").alias("steam_purchase"),
        F.col("review.received_for_free").alias("received_for_free"),
        F.col("review.timestamp_created").alias("timestamp_created"),
        F.col("review.votes_up").alias("votes_up"),
        F.col("review.votes_funny").alias("votes_funny"),
        F.col("review.weighted_vote_score").alias("weighted_vote_score"),
        recommendation_label.alias("recommendation_label"),
        (playtime_at_review.cast("double") / F.lit(60.0)).alias(
            "playtime_hours"
        ),
        F.lower(F.trim(F.col("review.language"))).alias("language_clean"),
        F.year(F.from_unixtime(F.col("review.timestamp_created"))).alias(
            "review_year"
        ),
    )
