"""Create static PNG charts from existing Gold Analytics Parquet datasets."""

from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from visualization.gold_charts import (
    plot_free_paid_recommendation,
    plot_genre_recommendation,
    plot_label_distribution,
    plot_playtime_recommendation,
    plot_purchase_recommendation,
    plot_top_games,
)


ANALYTICS_ROOT = "/steam/gold/analytics"
GAME_METRICS_PATH = f"{ANALYTICS_ROOT}/game_metrics"
GENRE_METRICS_PATH = f"{ANALYTICS_ROOT}/genre_metrics"
PLAYTIME_METRICS_PATH = f"{ANALYTICS_ROOT}/playtime_metrics"
FREE_PAID_METRICS_PATH = f"{ANALYTICS_ROOT}/free_paid_metrics"
LABEL_PROFILE_PATH = f"{ANALYTICS_ROOT}/label_profile"
PURCHASE_METRICS_PATH = f"{ANALYTICS_ROOT}/purchase_metrics"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_ROOT = PROJECT_ROOT / "evidence" / "visualization"
TOP_GAMES_OUTPUT = OUTPUT_ROOT / "top_games_recommendation.png"
GENRE_OUTPUT = OUTPUT_ROOT / "genre_recommendation.png"
PLAYTIME_OUTPUT = OUTPUT_ROOT / "playtime_recommendation.png"
FREE_PAID_OUTPUT = OUTPUT_ROOT / "free_paid_recommendation.png"
LABEL_OUTPUT = OUTPUT_ROOT / "label_distribution.png"
PURCHASE_OUTPUT = OUTPUT_ROOT / "purchase_recommendation.png"

EXPECTED_TOTAL_REVIEWS = 25_000
EXPECTED_POSITIVE_REVIEWS = 18_321
EXPECTED_NEGATIVE_REVIEWS = 6_679


def create_spark() -> SparkSession:
    return SparkSession.builder.appName("SteamGoldVisualizationV1").getOrCreate()


def _require_columns(
    frame: DataFrame,
    dataset_name: str,
    columns: Iterable[str],
) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise RuntimeError(
            f"{dataset_name} missing required columns: {', '.join(missing)}"
        )


def _require_non_empty(frame: DataFrame, dataset_name: str) -> int:
    count = frame.count()
    if count == 0:
        raise RuntimeError(f"{dataset_name} is empty")
    return count


def _validate_rate(frame: DataFrame, dataset_name: str) -> None:
    invalid = frame.filter(
        F.col("recommendation_rate").isNull()
        | (F.col("recommendation_rate") < 0)
        | (F.col("recommendation_rate") > 1)
    ).count()
    if invalid:
        raise RuntimeError(
            f"{dataset_name} contains {invalid} invalid recommendation rates"
        )


def _review_total(frame: DataFrame, dataset_name: str) -> int:
    total = frame.agg(F.sum("review_count").alias("total")).first()["total"]
    if total != EXPECTED_TOTAL_REVIEWS:
        raise RuntimeError(
            f"{dataset_name} review total {total} != {EXPECTED_TOTAL_REVIEWS}"
        )
    return total


def _require_groups(
    frame: DataFrame,
    dataset_name: str,
    column: str,
    expected: set[str],
) -> None:
    actual = {row[column] for row in frame.select(column).distinct().collect()}
    missing = sorted(expected - actual)
    if missing:
        raise RuntimeError(
            f"{dataset_name} missing expected groups: {', '.join(missing)}"
        )


def validate_analytics(datasets: dict[str, DataFrame]) -> dict[str, int]:
    game = datasets["game_metrics"]
    genre = datasets["genre_metrics"]
    playtime = datasets["playtime_metrics"]
    free_paid = datasets["free_paid_metrics"]
    profile = datasets["label_profile"]
    purchase = datasets["purchase_metrics"]

    _require_columns(
        game,
        "game_metrics",
        ("game_name", "review_count", "recommendation_rate"),
    )
    _require_columns(
        genre,
        "genre_metrics",
        ("genre", "review_count", "recommendation_rate"),
    )
    _require_columns(
        playtime,
        "playtime_metrics",
        ("playtime_bucket", "review_count", "recommendation_rate"),
    )
    _require_columns(
        free_paid,
        "free_paid_metrics",
        ("game_type", "game_count", "review_count", "recommendation_rate"),
    )
    _require_columns(
        profile,
        "label_profile",
        (
            "total_rows",
            "positive_count",
            "negative_count",
            "positive_percentage",
            "negative_percentage",
        ),
    )
    _require_columns(
        purchase,
        "purchase_metrics",
        ("purchase_source", "review_count", "recommendation_rate"),
    )

    counts = {
        "game_metrics": _require_non_empty(game, "game_metrics"),
        "genre_metrics": _require_non_empty(genre, "genre_metrics"),
        "playtime_metrics": _require_non_empty(playtime, "playtime_metrics"),
        "free_paid_metrics": _require_non_empty(
            free_paid, "free_paid_metrics"
        ),
        "label_profile": _require_non_empty(profile, "label_profile"),
        "purchase_metrics": _require_non_empty(purchase, "purchase_metrics"),
    }

    for name, frame in (
        ("game_metrics", game),
        ("genre_metrics", genre),
        ("playtime_metrics", playtime),
        ("free_paid_metrics", free_paid),
        ("purchase_metrics", purchase),
    ):
        _validate_rate(frame, name)

    _review_total(playtime, "playtime_metrics")
    _review_total(free_paid, "free_paid_metrics")
    _review_total(purchase, "purchase_metrics")
    _require_groups(
        free_paid,
        "free_paid_metrics",
        "game_type",
        {"FREE", "PAID"},
    )
    _require_groups(
        purchase,
        "purchase_metrics",
        "purchase_source",
        {"STEAM_PURCHASE", "OTHER_SOURCE"},
    )

    if counts["label_profile"] != 1:
        raise RuntimeError(
            "label_profile must contain exactly one logical profile row"
        )
    profile_row = profile.first()
    if profile_row["total_rows"] != EXPECTED_TOTAL_REVIEWS:
        raise RuntimeError("label_profile total_rows mismatch")
    if profile_row["positive_count"] != EXPECTED_POSITIVE_REVIEWS:
        raise RuntimeError("label_profile positive_count mismatch")
    if profile_row["negative_count"] != EXPECTED_NEGATIVE_REVIEWS:
        raise RuntimeError("label_profile negative_count mismatch")
    if (
        profile_row["positive_count"] + profile_row["negative_count"]
        != profile_row["total_rows"]
    ):
        raise RuntimeError("label_profile label counts do not reconcile")

    return counts


def read_analytics(spark: SparkSession) -> dict[str, DataFrame]:
    return {
        "game_metrics": spark.read.parquet(GAME_METRICS_PATH),
        "genre_metrics": spark.read.parquet(GENRE_METRICS_PATH),
        "playtime_metrics": spark.read.parquet(PLAYTIME_METRICS_PATH),
        "free_paid_metrics": spark.read.parquet(FREE_PAID_METRICS_PATH),
        "label_profile": spark.read.parquet(LABEL_PROFILE_PATH),
        "purchase_metrics": spark.read.parquet(PURCHASE_METRICS_PATH),
    }


def create_charts(datasets: dict[str, DataFrame]) -> list[Path]:
    outputs = [
        plot_top_games(
            datasets["game_metrics"].select(
                "game_name", "review_count", "recommendation_rate"
            ).toPandas(),
            TOP_GAMES_OUTPUT,
        ),
        plot_genre_recommendation(
            datasets["genre_metrics"].select(
                "genre", "review_count", "recommendation_rate"
            ).toPandas(),
            GENRE_OUTPUT,
        ),
        plot_playtime_recommendation(
            datasets["playtime_metrics"].select(
                "playtime_bucket", "review_count", "recommendation_rate"
            ).toPandas(),
            PLAYTIME_OUTPUT,
        ),
        plot_free_paid_recommendation(
            datasets["free_paid_metrics"].select(
                "game_type", "game_count", "review_count", "recommendation_rate"
            ).toPandas(),
            FREE_PAID_OUTPUT,
        ),
        plot_label_distribution(
            datasets["label_profile"].select(
                "positive_count",
                "negative_count",
                "positive_percentage",
                "negative_percentage",
            ).toPandas(),
            LABEL_OUTPUT,
        ),
        plot_purchase_recommendation(
            datasets["purchase_metrics"].select(
                "purchase_source", "review_count", "recommendation_rate"
            ).toPandas(),
            PURCHASE_OUTPUT,
        ),
    ]
    for output in outputs:
        if not output.is_file() or output.stat().st_size == 0:
            raise RuntimeError(f"Visualization output is missing or empty: {output}")
    return outputs


def main() -> None:
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        datasets = read_analytics(spark)
        counts = validate_analytics(datasets)
        outputs = create_charts(datasets)

        print("\n=== VISUALIZATION V1 ===")
        print(
            "Validated aggregate rows: "
            + ", ".join(f"{name}={count}" for name, count in counts.items())
        )
        print("\nCreated:")
        for output in outputs:
            print(output.relative_to(PROJECT_ROOT))
        print("\nVISUALIZATION VALIDATION: PASS")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
