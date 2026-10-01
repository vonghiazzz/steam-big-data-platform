from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from analytics.gold_analytics import (
    recommendation_by_game,
    recommendation_by_genre,
    recommendation_by_playtime,
    free_vs_paid,
    engagement_by_game,
    label_profile,
    recommendation_by_platform,
    recommendation_by_category,
    recommendation_by_purchase_source,
)


GOLD_BASE_PATH = "/steam/gold/base"

ANALYTICS_ROOT = "/steam/gold/analytics"

GAME_METRICS_PATH = f"{ANALYTICS_ROOT}/game_metrics"
GENRE_METRICS_PATH = f"{ANALYTICS_ROOT}/genre_metrics"
PLAYTIME_METRICS_PATH = f"{ANALYTICS_ROOT}/playtime_metrics"
FREE_PAID_METRICS_PATH = f"{ANALYTICS_ROOT}/free_paid_metrics"
ENGAGEMENT_METRICS_PATH = f"{ANALYTICS_ROOT}/engagement_metrics"
LABEL_PROFILE_PATH = f"{ANALYTICS_ROOT}/label_profile"
PLATFORM_METRICS_PATH = f"{ANALYTICS_ROOT}/platform_metrics"
CATEGORY_METRICS_PATH = f"{ANALYTICS_ROOT}/category_metrics"
PURCHASE_METRICS_PATH = f"{ANALYTICS_ROOT}/purchase_metrics"

def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SteamGoldAnalyticsV1")
        .getOrCreate()
    )

def validate_gold(gold_df):
    row_count = gold_df.count()

    unique_reviews = (
        gold_df
        .select("recommendationid")
        .distinct()
        .count()
    )

    distinct_games = (
        gold_df
        .select("appid")
        .distinct()
        .count()
    )

    label_counts = {
        row["voted_up"]: row["count"]
        for row in gold_df.groupBy("voted_up").count().collect()
    }

    positive_count = label_counts.get(True, 0)
    negative_count = label_counts.get(False, 0)

    print("\n=== GOLD BASE VALIDATION ===")
    print(f"rows={row_count}")
    print(f"unique_recommendationid={unique_reviews}")
    print(f"distinct_appids={distinct_games}")
    print(f"voted_up=true count={positive_count}")
    print(f"voted_up=false count={negative_count}")

    if row_count <= 0:
        raise RuntimeError("Gold Base is empty")

    if unique_reviews != row_count:
        raise RuntimeError(
            "Gold recommendationid values are not unique: "
            f"rows={row_count}, unique={unique_reviews}"
        )

    if distinct_games <= 0:
        raise RuntimeError("Gold Base contains no game appids")

    if positive_count + negative_count != row_count:
        raise RuntimeError("Gold Base contains null or invalid voted_up values")

    return {
        "rows": row_count,
        "games": distinct_games,
        "positive": positive_count,
        "negative": negative_count,
    }


def write_analytics(
    game_metrics,
    genre_metrics,
    playtime_metrics,
    free_paid_metrics,
    engagement_metrics,
    profile,
    platform_metrics,
    category_metrics,
    purchase_metrics
):
    outputs = [
        (game_metrics, GAME_METRICS_PATH),
        (genre_metrics, GENRE_METRICS_PATH),
        (playtime_metrics, PLAYTIME_METRICS_PATH),
        (free_paid_metrics, FREE_PAID_METRICS_PATH),
        (engagement_metrics, ENGAGEMENT_METRICS_PATH),
        (profile, LABEL_PROFILE_PATH),
        (platform_metrics, PLATFORM_METRICS_PATH),
        (category_metrics, CATEGORY_METRICS_PATH),
        (purchase_metrics, PURCHASE_METRICS_PATH),
    ]

    print("\n=== WRITING ANALYTICS TO HDFS ===")

    for df, path in outputs:
        print(f"Writing: {path}")

        (
            df
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(path)
        )

def validate_written_analytics(
    spark: SparkSession,
    *,
    expected_rows: int,
    expected_games: int,
    expected_positive: int,
    expected_negative: int,
):
    print("\n=== READ-BACK VALIDATION ===")

    game_df = spark.read.parquet(GAME_METRICS_PATH)
    genre_df = spark.read.parquet(GENRE_METRICS_PATH)
    playtime_df = spark.read.parquet(PLAYTIME_METRICS_PATH)
    free_paid_df = spark.read.parquet(FREE_PAID_METRICS_PATH)
    engagement_df = spark.read.parquet(ENGAGEMENT_METRICS_PATH)
    profile_df = spark.read.parquet(LABEL_PROFILE_PATH)
    platform_df = spark.read.parquet(PLATFORM_METRICS_PATH)
    category_df = spark.read.parquet(CATEGORY_METRICS_PATH)
    purchase_df = spark.read.parquet(PURCHASE_METRICS_PATH)

    game_count = game_df.count()
    engagement_count = engagement_df.count()

    platform_count = platform_df.count()
    category_count = category_df.count()
    purchase_count = purchase_df.count()

    purchase_total = (
        purchase_df
        .agg(F.sum("review_count").alias("total"))
        .first()["total"]
    )

    invalid_platform_rates = (
        platform_df
        .filter(
            (F.col("recommendation_rate") < 0)
            | (F.col("recommendation_rate") > 1)
        )
        .count()
    )

    invalid_category_rates = (
        category_df
        .filter(
            (F.col("recommendation_rate") < 0)
            | (F.col("recommendation_rate") > 1)
        )
        .count()
    )

    invalid_purchase_rates = (
        purchase_df
        .filter(
            (F.col("recommendation_rate") < 0)
            | (F.col("recommendation_rate") > 1)
        )
        .count()
    )

    playtime_total = (
        playtime_df
        .agg(F.sum("review_count").alias("total"))
        .first()["total"]
    )

    free_paid_total = (
        free_paid_df
        .agg(F.sum("review_count").alias("total"))
        .first()["total"]
    )

    profile_row = profile_df.first()

    invalid_game_rates = (
        game_df
        .filter(
            (F.col("recommendation_rate") < 0)
            | (F.col("recommendation_rate") > 1)
        )
        .count()
    )

    invalid_genre_rates = (
        genre_df
        .filter(
            (F.col("recommendation_rate") < 0)
            | (F.col("recommendation_rate") > 1)
        )
        .count()
    )

    unknown_genre_count = (
        genre_df
        .filter(F.col("genre") == "UNKNOWN")
        .select("review_count")
        .first()
    )

    print(f"game_metrics rows={game_count}")
    print(f"engagement_metrics rows={engagement_count}")
    print(f"playtime review total={playtime_total}")
    print(f"free/paid review total={free_paid_total}")
    print(f"invalid game rates={invalid_game_rates}")
    print(f"invalid genre rates={invalid_genre_rates}")
    print(f"platform_metrics rows={platform_count}")
    print(f"category_metrics rows={category_count}")
    print(f"purchase_metrics rows={purchase_count}")
    print(f"purchase review total={purchase_total}")

    print(f"invalid platform rates={invalid_platform_rates}")
    print(f"invalid category rates={invalid_category_rates}")
    print(f"invalid purchase rates={invalid_purchase_rates}")
    if unknown_genre_count:
        print(
            "UNKNOWN genre reviews="
            f"{unknown_genre_count['review_count']}"
        )

    print(
        f"profile total_rows={profile_row['total_rows']}, "
        f"positive={profile_row['positive_count']}, "
        f"negative={profile_row['negative_count']}"
    )

    if game_count != expected_games:
        raise RuntimeError(
            f"Expected {expected_games} game metric rows, found {game_count}"
        )

    if engagement_count != expected_games:
        raise RuntimeError(
            f"Expected {expected_games} engagement rows, found {engagement_count}"
        )

    if playtime_total != expected_rows:
        raise RuntimeError(
            f"Playtime metrics do not reconcile: {playtime_total}"
        )

    if free_paid_total != expected_rows:
        raise RuntimeError(
            f"Free/Paid metrics do not reconcile: {free_paid_total}"
        )

    if invalid_game_rates != 0:
        raise RuntimeError("Invalid recommendation rate in game metrics")

    if invalid_genre_rates != 0:
        raise RuntimeError("Invalid recommendation rate in genre metrics")

    if profile_row["total_rows"] != expected_rows:
        raise RuntimeError("Label profile total mismatch")

    if profile_row["positive_count"] != expected_positive:
        raise RuntimeError("Positive label count mismatch")

    if profile_row["negative_count"] != expected_negative:
        raise RuntimeError("Negative label count mismatch")

    if platform_count == 0:
        raise RuntimeError("Platform metrics are empty")

    if category_count == 0:
        raise RuntimeError("Category metrics are empty")

    if purchase_count == 0:
        raise RuntimeError("Purchase metrics are empty")

    if purchase_total != expected_rows:
        raise RuntimeError(
            f"Purchase metrics do not reconcile: {purchase_total}"
        )

    if invalid_platform_rates != 0:
        raise RuntimeError(
            "Invalid recommendation rate in platform metrics"
        )

    if invalid_category_rates != 0:
        raise RuntimeError(
            "Invalid recommendation rate in category metrics"
        )

    if invalid_purchase_rates != 0:
        raise RuntimeError(
            "Invalid recommendation rate in purchase metrics"
        )
    print("READ-BACK VALIDATION: PASS")


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        gold_df = spark.read.parquet(GOLD_BASE_PATH)

        gold_df.printSchema()

        expected = validate_gold(gold_df)

        print("\n=== DATA QUALITY CHECK ===")

        print("\nGames with missing genres:")
        (
            gold_df
            .filter(F.col("genres").isNull())
            .select("appid", "game_name")
            .distinct()
            .show(truncate=False)
        )

        invalid_playtime = (
            gold_df
            .filter(
                F.col("playtime_at_review").isNotNull()
                & F.col("playtime_forever").isNotNull()
                & (
                    F.col("playtime_forever")
                    < F.col("playtime_at_review")
                )
            )
        )

        print(
            "rows where playtime_forever < playtime_at_review =",
            invalid_playtime.count()
        )

        (
            invalid_playtime
            .groupBy("appid", "game_name")
            .count()
            .orderBy(F.desc("count"))
            .show(20, truncate=False)
        )

        print("\nInvalid playtime rows:")

        (
            invalid_playtime
            .select(
                "recommendationid",
                "appid",
                "game_name",
                "playtime_at_review",
                "playtime_forever",
            )
            .show(truncate=False)
        )

        gold_df.createOrReplaceTempView("steam_gold")

        game_metrics = spark.sql("""
            SELECT
                appid,
                game_name,
                COUNT(*) AS review_count,
                SUM(CASE WHEN voted_up = true THEN 1 ELSE 0 END)
                    AS positive_reviews,
                SUM(CASE WHEN voted_up = false THEN 1 ELSE 0 END)
                    AS negative_reviews,
                SUM(CASE WHEN voted_up = true THEN 1 ELSE 0 END)
                    / CAST(COUNT(*) AS DOUBLE)
                    AS recommendation_rate
            FROM steam_gold
            GROUP BY appid, game_name
        """)

        genre_metrics = recommendation_by_genre(gold_df)

        playtime_metrics = recommendation_by_playtime(gold_df)

        free_paid_metrics = free_vs_paid(gold_df)

        engagement_metrics = engagement_by_game(gold_df)

        profile = label_profile(gold_df)

        platform_metrics = recommendation_by_platform(gold_df)

        category_metrics = recommendation_by_category(gold_df)

        purchase_metrics = recommendation_by_purchase_source(gold_df)

        print("\n=== GAME METRICS ===")
        game_metrics.orderBy(
            F.desc("recommendation_rate")
        ).show(20, truncate=False)

        print("\n=== GENRE METRICS ===")
        genre_metrics.orderBy(
            F.desc("review_count")
        ).show(20, truncate=False)

        print("\n=== PLAYTIME METRICS ===")
        playtime_metrics.show(truncate=False)

        print("\n=== FREE VS PAID ===")
        free_paid_metrics.show(truncate=False)

        print("\n=== ENGAGEMENT ===")
        engagement_metrics.orderBy(
            F.desc("review_count")
        ).show(20, truncate=False)

        print("\n=== LABEL PROFILE ===")
        profile.show(truncate=False)

        print("\n=== PLATFORM METRICS ===")
        platform_metrics.orderBy(
            F.desc("review_count")
        ).show(truncate=False)

        print("\n=== CATEGORY METRICS ===")
        category_metrics.orderBy(
            F.desc("review_count")
        ).show(50, truncate=False)

        print("\n=== PURCHASE METRICS ===")
        purchase_metrics.show(truncate=False)

        write_analytics(
            game_metrics,
            genre_metrics,
            playtime_metrics,
            free_paid_metrics,
            engagement_metrics,
            profile,
            platform_metrics,
            category_metrics,
            purchase_metrics,
        )

        validate_written_analytics(
            spark,
            expected_rows=expected["rows"],
            expected_games=expected["games"],
            expected_positive=expected["positive"],
            expected_negative=expected["negative"],
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
