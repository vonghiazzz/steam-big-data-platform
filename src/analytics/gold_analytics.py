from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def recommendation_by_game(gold_df: DataFrame) -> DataFrame:
    return (
        gold_df
        .groupBy("appid", "game_name")
        .agg(
            F.count("*").alias("review_count"),
            F.sum(
                F.when(F.col("voted_up") == True, 1).otherwise(0)
            ).alias("positive_reviews"),
            F.sum(
                F.when(F.col("voted_up") == False, 1).otherwise(0)
            ).alias("negative_reviews"),
        )
        .withColumn(
            "recommendation_rate",
            F.col("positive_reviews") / F.col("review_count")
        )
    )

def recommendation_by_genre(gold_df: DataFrame) -> DataFrame:
    exploded = (
        gold_df
        .select(
            "recommendationid",
            "voted_up",
            F.explode_outer("genres").alias("genre")
        )
        .withColumn(
            "genre",
            F.coalesce(F.col("genre"), F.lit("UNKNOWN"))
        )
        .dropDuplicates(["recommendationid", "genre"])
    )

    return (
        exploded
        .groupBy("genre")
        .agg(
            F.countDistinct("recommendationid").alias("review_count"),
            F.sum(
                F.when(F.col("voted_up") == True, 1).otherwise(0)
            ).alias("positive_reviews"),
            F.sum(
                F.when(F.col("voted_up") == False, 1).otherwise(0)
            ).alias("negative_reviews"),
        )
        .withColumn(
            "recommendation_rate",
            F.col("positive_reviews") / F.col("review_count")
        )
    )

def recommendation_by_playtime(gold_df: DataFrame) -> DataFrame:
    return (
        gold_df
        .groupBy("playtime_bucket")
        .agg(
            F.count("*").alias("review_count"),
            F.sum(
                F.when(F.col("voted_up") == True, 1).otherwise(0)
            ).alias("positive_reviews"),
            F.sum(
                F.when(F.col("voted_up") == False, 1).otherwise(0)
            ).alias("negative_reviews"),
            F.avg("playtime_hours").alias("avg_playtime_hours"),
        )
        .withColumn(
            "recommendation_rate",
            F.col("positive_reviews") / F.col("review_count")
        )
    )


def free_vs_paid(gold_df: DataFrame) -> DataFrame:
    review_metrics = (
        gold_df
        .withColumn(
            "game_type",
            F.when(F.col("is_free") == True, "FREE").otherwise("PAID")
        )
        .groupBy("game_type")
        .agg(
            F.countDistinct("appid").alias("game_count"),
            F.count("*").alias("review_count"),
            F.count("playtime_hours").alias(
                "playtime_observed_count"
            ),
            F.sum(
                F.when(F.col("voted_up") == True, 1).otherwise(0)
            ).alias("positive_reviews"),
            F.sum(
                F.when(F.col("voted_up") == False, 1).otherwise(0)
            ).alias("negative_reviews"),
            F.avg("playtime_hours").alias("avg_playtime_hours"),
        )
        .withColumn(
            "recommendation_rate",
            F.col("positive_reviews") / F.col("review_count")
        )
    )

    game_prices = (
        gold_df
        .select("appid", "is_free", "price")
        .dropDuplicates(["appid"])
        .withColumn(
            "game_type",
            F.when(F.col("is_free") == True, "FREE").otherwise("PAID")
        )
        .groupBy("game_type")
        .agg(
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.avg("price").alias("avg_price"),
        )
    )

    return review_metrics.join(
        game_prices,
        on="game_type",
        how="left"
    )

def recommendation_by_platform(gold_df: DataFrame) -> DataFrame:
    platform_rows = (
        gold_df
        .select(
            "recommendationid",
            "voted_up",
            F.array(
                F.when(
                    F.col("platforms.windows") == True,
                    F.lit("WINDOWS")
                ),
                F.when(
                    F.col("platforms.mac") == True,
                    F.lit("MAC")
                ),
                F.when(
                    F.col("platforms.linux") == True,
                    F.lit("LINUX")
                ),
            ).alias("platform_list")
        )
        .select(
            "recommendationid",
            "voted_up",
            F.explode("platform_list").alias("platform")
        )
        .filter(F.col("platform").isNotNull())
    )

    return (
        platform_rows
        .groupBy("platform")
        .agg(
            F.countDistinct("recommendationid").alias("review_count"),
            F.sum(
                F.when(F.col("voted_up") == True, 1).otherwise(0)
            ).alias("positive_reviews"),
            F.sum(
                F.when(F.col("voted_up") == False, 1).otherwise(0)
            ).alias("negative_reviews"),
        )
        .withColumn(
            "recommendation_rate",
            F.col("positive_reviews") / F.col("review_count")
        )
    )

def recommendation_by_purchase_source(
    gold_df: DataFrame
) -> DataFrame:
    return (
        gold_df
        .withColumn(
            "purchase_source",
            F.when(
                F.col("steam_purchase") == True,
                "STEAM_PURCHASE"
            ).otherwise("OTHER_SOURCE")
        )
        .groupBy("purchase_source")
        .agg(
            F.count("*").alias("review_count"),
            F.sum(
                F.when(F.col("voted_up") == True, 1).otherwise(0)
            ).alias("positive_reviews"),
            F.sum(
                F.when(F.col("voted_up") == False, 1).otherwise(0)
            ).alias("negative_reviews"),
            F.avg("playtime_hours").alias("avg_playtime_hours"),
        )
        .withColumn(
            "recommendation_rate",
            F.col("positive_reviews") / F.col("review_count")
        )
    )

def recommendation_by_received_for_free(
    gold_df: DataFrame
) -> DataFrame:
    return (
        gold_df
        .withColumn(
            "acquisition_type",
            F.when(
                F.col("received_for_free") == True,
                "RECEIVED_FOR_FREE"
            ).otherwise("NOT_RECEIVED_FOR_FREE")
        )
        .groupBy("acquisition_type")
        .agg(
            F.count("*").alias("review_count"),
            F.sum(
                F.when(F.col("voted_up") == True, 1).otherwise(0)
            ).alias("positive_reviews"),
            F.sum(
                F.when(F.col("voted_up") == False, 1).otherwise(0)
            ).alias("negative_reviews"),
            F.avg("playtime_hours").alias("avg_playtime_hours"),
        )
        .withColumn(
            "recommendation_rate",
            F.col("positive_reviews") / F.col("review_count")
        )
    )

def recommendation_by_price_bucket(
    gold_df: DataFrame
) -> DataFrame:
    df = (
        gold_df
        .withColumn(
            "price_bucket",
            F.when(
                F.col("is_free") == True,
                "FREE"
            )
            .when(
                F.col("price") < 200000,
                "<200K"
            )
            .when(
                F.col("price") < 500000,
                "200K-500K"
            )
            .when(
                F.col("price") < 1000000,
                "500K-1M"
            )
            .otherwise("1M+")
        )
    )

    return (
        df
        .groupBy("price_bucket")
        .agg(
            F.countDistinct("appid").alias("game_count"),
            F.count("*").alias("review_count"),
            F.sum(
                F.when(F.col("voted_up") == True, 1).otherwise(0)
            ).alias("positive_reviews"),
            F.avg("playtime_hours").alias("avg_playtime_hours"),
        )
        .withColumn(
            "recommendation_rate",
            F.col("positive_reviews") / F.col("review_count")
        )
    )

def recommendation_by_category(
    gold_df: DataFrame
) -> DataFrame:
    exploded = (
        gold_df
        .select(
            "recommendationid",
            "voted_up",
            F.explode_outer("categories").alias("category")
        )
        .withColumn(
            "category",
            F.coalesce(F.col("category"), F.lit("UNKNOWN"))
        )
        .dropDuplicates(["recommendationid", "category"])
    )

    return (
        exploded
        .groupBy("category")
        .agg(
            F.countDistinct("recommendationid").alias("review_count"),
            F.sum(
                F.when(F.col("voted_up") == True, 1).otherwise(0)
            ).alias("positive_reviews"),
            F.sum(
                F.when(F.col("voted_up") == False, 1).otherwise(0)
            ).alias("negative_reviews"),
        )
        .withColumn(
            "recommendation_rate",
            F.col("positive_reviews") / F.col("review_count")
        )
    )

def engagement_by_game(gold_df: DataFrame) -> DataFrame:
    return (
        gold_df
        .groupBy("appid", "game_name")
        .agg(
            F.count("*").alias("review_count"),

            F.count("playtime_at_review").alias(
                "playtime_at_review_observed_count"
            ),

            F.count("playtime_forever").alias(
                "playtime_forever_observed_count"
            ),

            F.avg(
                F.col("playtime_at_review") / F.lit(60.0)
            ).alias("avg_playtime_at_review_hours"),

            F.avg(
                F.col("playtime_forever") / F.lit(60.0)
            ).alias("avg_playtime_forever_hours"),

            F.avg("votes_up").alias("avg_votes_up"),
            F.avg("votes_funny").alias("avg_votes_funny"),

            F.avg(
                F.col("weighted_vote_score").cast("double")
            ).alias("avg_weighted_vote_score"),

            F.sum(
                F.when(F.col("voted_up") == True, 1).otherwise(0)
            ).alias("positive_reviews"),
        )
        .withColumn(
            "recommendation_rate",
            F.col("positive_reviews") / F.col("review_count")
        )
    )


def label_profile(gold_df: DataFrame) -> DataFrame:
    return (
        gold_df
        .agg(
            F.count("*").alias("total_rows"),
            F.countDistinct("recommendationid").alias(
                "unique_recommendationid"
            ),
            F.countDistinct("appid").alias("distinct_appids"),

            F.sum(
                F.when(F.col("voted_up") == True, 1).otherwise(0)
            ).alias("positive_count"),

            F.sum(
                F.when(F.col("voted_up") == False, 1).otherwise(0)
            ).alias("negative_count"),

            F.sum(
                F.when(F.col("is_free") == True, 1).otherwise(0)
            ).alias("free_review_count"),

            F.sum(
                F.when(F.col("is_free") == False, 1).otherwise(0)
            ).alias("paid_review_count"),

            F.sum(
                F.when(F.col("playtime_at_review").isNull(), 1)
                .otherwise(0)
            ).alias("null_playtime_at_review"),

            F.sum(
                F.when(F.col("playtime_forever").isNull(), 1)
                .otherwise(0)
            ).alias("null_playtime_forever"),

            F.sum(
                F.when(F.col("price").isNull(), 1).otherwise(0)
            ).alias("null_price"),

            F.sum(
                F.when(F.col("genres").isNull(), 1).otherwise(0)
            ).alias("null_genres"),

            F.sum(
                F.when(F.col("timestamp_created").isNull(), 1)
                .otherwise(0)
            ).alias("null_timestamp_created"),

            F.sum(
                F.when(F.col("weighted_vote_score").isNull(), 1)
                .otherwise(0)
            ).alias("null_weighted_vote_score"),
        )
        .withColumn(
            "positive_percentage",
            F.col("positive_count") / F.col("total_rows")
        )
        .withColumn(
            "negative_percentage",
            F.col("negative_count") / F.col("total_rows")
        )
    )