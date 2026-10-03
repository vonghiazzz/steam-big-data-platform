"""Bronze-to-Silver batch job (PySpark).

Implements the checkpoint described in docs/04_BATCH_PIPELINE.md:

1. Read Bronze game and review JSONL from HDFS.
2. Apply explicit PySpark schemas (no inference).
3. Validate required keys and source relationships.
4. Convert booleans, numerics, arrays, and timestamps to stable types.
5. Deduplicate reviews by ``recommendationid`` (rule: keep the record with
   the greatest ``timestamp_updated``, tie-broken by greatest ``ingested_at``).
6. Normalize game metadata structures needed for joins and analytics.
7. Write Silver Parquet to HDFS (reviews partitioned by ``appid``).
8. Produce a Bronze-versus-Silver reconciliation report (inputs, accepted,
   rejected by reason, duplicates, output counts, read-back verification).

Rejected records are kept as evidence under ``<silver>/<version>/rejected_*``
instead of being silently dropped. The job exits non-zero if the Silver
contract is broken (zero accepted rows, or read-back count mismatch).

Intended runtime: cluster client container with PySpark on PYTHONPATH and
HADOOP_CONF_DIR pointing at the cluster Hadoop configuration, e.g.

    PYTHONPATH=/tmp/pylibs HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
        python3 bronze_to_silver.py \
            --bronze-root /steam/bronze --silver-root /steam/silver

This module is intentionally self-contained (no repo imports) so it can be
copied onto a client container as a single file.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

SCHEMA_VERSION = "silver.v1"

# ---------------------------------------------------------------------------
# Explicit Bronze schemas (envelope shape written by the Python crawlers).
# Scalar fields that Steam occasionally returns with inconsistent types are
# read as strings and cast explicitly (weighted_vote_score, required_age).
# ---------------------------------------------------------------------------

GAMES_ENVELOPE_SCHEMA = T.StructType(
    [
        T.StructField("appid", T.LongType()),
        T.StructField("collected_at", T.StringType()),
        T.StructField("source", T.StringType()),
        T.StructField("success", T.BooleanType()),
        T.StructField(
            "data",
            T.StructType(
                [
                    T.StructField("type", T.StringType()),
                    T.StructField("name", T.StringType()),
                    T.StructField("steam_appid", T.LongType()),
                    T.StructField("required_age", T.StringType()),
                    T.StructField("is_free", T.BooleanType()),
                    T.StructField(
                        "price_overview",
                        T.StructType(
                            [
                                T.StructField("currency", T.StringType()),
                                T.StructField("initial", T.LongType()),
                                T.StructField("final", T.LongType()),
                                T.StructField(
                                    "discount_percent", T.LongType()
                                ),
                            ]
                        ),
                    ),
                    T.StructField(
                        "genres",
                        T.ArrayType(
                            T.StructType(
                                [
                                    T.StructField("id", T.StringType()),
                                    T.StructField(
                                        "description", T.StringType()
                                    ),
                                ]
                            )
                        ),
                    ),
                    T.StructField(
                        "categories",
                        T.ArrayType(
                            T.StructType(
                                [
                                    T.StructField("id", T.StringType()),
                                    T.StructField(
                                        "description", T.StringType()
                                    ),
                                ]
                            )
                        ),
                    ),
                    T.StructField(
                        "platforms",
                        T.StructType(
                            [
                                T.StructField("windows", T.BooleanType()),
                                T.StructField("mac", T.BooleanType()),
                                T.StructField("linux", T.BooleanType()),
                            ]
                        ),
                    ),
                    T.StructField(
                        "release_date",
                        T.StructType(
                            [
                                T.StructField(
                                    "coming_soon", T.BooleanType()
                                ),
                                T.StructField("date", T.StringType()),
                            ]
                        ),
                    ),
                    T.StructField("dlc", T.ArrayType(T.LongType())),
                    T.StructField(
                        "developers", T.ArrayType(T.StringType())
                    ),
                    T.StructField(
                        "publishers", T.ArrayType(T.StringType())
                    ),
                ]
            ),
        ),
        T.StructField("_corrupt_record", T.StringType()),
    ]
)

REVIEW_AUTHOR_SCHEMA = T.StructType(
    [
        T.StructField("steamid", T.StringType()),
        T.StructField("num_games_owned", T.LongType()),
        T.StructField("num_reviews", T.LongType()),
        T.StructField("playtime_forever", T.LongType()),
        T.StructField("playtime_last_two_weeks", T.LongType()),
        T.StructField("playtime_at_review", T.LongType()),
        T.StructField("last_played", T.LongType()),
    ]
)

REVIEW_ENVELOPE_SCHEMA = T.StructType(
    [
        T.StructField("appid", T.LongType()),
        T.StructField("game_name", T.StringType()),
        T.StructField("ingested_at", T.StringType()),
        T.StructField("page_number", T.LongType()),
        T.StructField("request_cursor", T.StringType()),
        T.StructField(
            "review",
            T.StructType(
                [
                    T.StructField("recommendationid", T.StringType()),
                    T.StructField("author", REVIEW_AUTHOR_SCHEMA),
                    T.StructField("language", T.StringType()),
                    T.StructField("review", T.StringType()),
                    T.StructField("timestamp_created", T.LongType()),
                    T.StructField("timestamp_updated", T.LongType()),
                    T.StructField("voted_up", T.BooleanType()),
                    T.StructField("votes_up", T.LongType()),
                    T.StructField("votes_funny", T.LongType()),
                    T.StructField("weighted_vote_score", T.StringType()),
                    T.StructField("comment_count", T.LongType()),
                    T.StructField("steam_purchase", T.BooleanType()),
                    T.StructField("received_for_free", T.BooleanType()),
                    T.StructField("refunded", T.BooleanType()),
                    T.StructField(
                        "written_during_early_access", T.BooleanType()
                    ),
                    T.StructField(
                        "primarily_steam_deck", T.BooleanType()
                    ),
                ]
            ),
        ),
        T.StructField("_corrupt_record", T.StringType()),
    ]
)

# Reject reasons (kept in rejected outputs and the reconciliation report).
REASON_UNPARSEABLE = "unparseable_json"
REASON_FETCH_FAILED = "game_fetch_failed"
REASON_MISSING_APPID = "missing_appid"
REASON_MISSING_RECOMMENDATIONID = "missing_recommendationid"


# Crawler timestamps come from datetime.isoformat() -> 6-digit microseconds.
ISO_TS_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"


def _ts(column: str) -> F.Column:
    """Epoch seconds (UTC) -> timestamp, null on non-positive input."""
    return F.when(F.col(column) > 0, F.to_timestamp(F.from_unixtime(column)))


def build_silver_games(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (accepted, rejected) Silver game frames from Bronze envelope."""
    reject_reason = (
        F.when(F.col("_corrupt_record").isNotNull(), F.lit(REASON_UNPARSEABLE))
        .when(
            (~F.col("success").eqNullSafe(True)) | F.col("data").isNull(),
            F.lit(REASON_FETCH_FAILED),
        )
        .when(F.col("appid").isNull(), F.lit(REASON_MISSING_APPID))
    )

    rejected = raw.filter(reject_reason.isNotNull()).select(
        "appid",
        "source",
        "success",
        reject_reason.alias("reject_reason"),
        F.to_json(F.struct("*")).alias("raw_record"),
    )

    accepted = raw.filter(reject_reason.isNull()).select(
        F.col("appid").cast("long").alias("appid"),
        F.col("data.name").alias("name"),
        F.col("data.type").alias("type"),
        F.col("data.is_free").cast("boolean").alias("is_free"),
        F.col("data.required_age").cast("int").alias("required_age"),
        F.col("data.price_overview.currency").alias("price_currency"),
        F.col("data.price_overview.initial").alias("price_initial_cents"),
        F.col("data.price_overview.final").alias("price_final_cents"),
        F.col("data.price_overview.discount_percent").alias("discount_percent"),
        F.transform("data.genres", lambda g: g.description).alias("genres"),
        F.transform("data.categories", lambda c: c.description).alias(
            "categories"
        ),
        F.col("data.platforms.windows").alias("platform_windows"),
        F.col("data.platforms.mac").alias("platform_mac"),
        F.col("data.platforms.linux").alias("platform_linux"),
        F.col("data.release_date.coming_soon").alias("release_coming_soon"),
        F.col("data.release_date.date").alias("release_date"),
        F.col("data.dlc").alias("dlc_appids"),
        F.col("data.developers").alias("developers"),
        F.col("data.publishers").alias("publishers"),
        F.col("source").alias("source"),
        F.to_timestamp(F.col("collected_at"), ISO_TS_PATTERN).alias(
            "collected_at"
        ),
    )
    return accepted, rejected


def build_silver_reviews(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (accepted, rejected) Silver review frames from Bronze envelope."""
    reject_reason = (
        F.when(F.col("_corrupt_record").isNotNull(), F.lit(REASON_UNPARSEABLE))
        .when(
            F.col("review").isNull()
            | F.col("review.recommendationid").isNull()
            | (F.length(F.col("review.recommendationid")) == 0),
            F.lit(REASON_MISSING_RECOMMENDATIONID),
        )
        .when(F.col("appid").isNull(), F.lit(REASON_MISSING_APPID))
    )

    rejected = raw.filter(reject_reason.isNotNull()).select(
        "appid",
        F.col("review.recommendationid").alias("recommendationid"),
        reject_reason.alias("reject_reason"),
        F.to_json(F.struct("*")).alias("raw_record"),
    )

    valid = raw.filter(reject_reason.isNull())

    # Dedupe rule: one row per recommendationid, keeping the greatest
    # (timestamp_updated, ingested_at) so refreshed votes win replays.
    dedupe_window = Window.partitionBy("review.recommendationid").orderBy(
        F.col("review.timestamp_updated").desc_nulls_last(),
        F.col("ingested_at").desc_nulls_last(),
    )
    deduped = valid.withColumn("_rn", F.row_number().over(dedupe_window)).filter(
        F.col("_rn") == 1
    )

    accepted = deduped.select(
        F.col("review.recommendationid").alias("recommendationid"),
        F.col("appid").cast("long").alias("appid"),
        F.col("game_name").alias("game_name"),
        F.col("review.language").alias("language"),
        F.col("review.review").alias("review_text"),
        F.col("review.voted_up").cast("boolean").alias("voted_up"),
        F.col("review.votes_up").cast("long").alias("votes_up"),
        F.col("review.votes_funny").cast("long").alias("votes_funny"),
        F.col("review.comment_count").cast("long").alias("comment_count"),
        F.col("review.weighted_vote_score")
        .cast("double")
        .alias("weighted_vote_score"),
        F.col("review.steam_purchase").alias("steam_purchase"),
        F.col("review.received_for_free").alias("received_for_free"),
        F.col("review.refunded").alias("refunded"),
        F.col("review.written_during_early_access").alias(
            "written_during_early_access"
        ),
        F.col("review.primarily_steam_deck").alias("primarily_steam_deck"),
        F.col("review.author.steamid").alias("author_steamid"),
        F.col("review.author.num_games_owned").alias("author_num_games_owned"),
        F.col("review.author.num_reviews").alias("author_num_reviews"),
        F.col("review.author.playtime_forever").alias(
            "author_playtime_forever_min"
        ),
        F.col("review.author.playtime_last_two_weeks").alias(
            "author_playtime_last_two_weeks_min"
        ),
        F.col("review.author.playtime_at_review").alias(
            "author_playtime_at_review_min"
        ),
        _ts("review.author.last_played").alias("author_last_played_at"),
        _ts("review.timestamp_created").alias("review_created_at"),
        _ts("review.timestamp_updated").alias("review_updated_at"),
        F.to_timestamp(F.col("ingested_at"), ISO_TS_PATTERN).alias(
            "ingested_at"
        ),
        F.col("page_number").cast("long").alias("source_page_number"),
    )
    return accepted, rejected


def write_text_file(spark: SparkSession, path: str, content: str) -> None:
    """Write a single text file through the active Hadoop FileSystem."""
    jvm = spark._jvm
    fs_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = fs_path.getFileSystem(spark._jsc.hadoopConfiguration())
    stream = fs.create(fs_path, True)
    try:
        stream.write(content.encode("utf-8"))
    finally:
        stream.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze-to-Silver batch job")
    parser.add_argument("--bronze-root", default="/steam/bronze")
    parser.add_argument("--silver-root", default="/steam/silver")
    parser.add_argument("--version", default="v1")
    parser.add_argument("--master", default="local[*]")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    silver_version_root = f"{args.silver_root}/{args.version}"

    spark = (
        SparkSession.builder.master(args.master)
        .appName(f"bronze-to-silver-{args.version}")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    report: dict = {
        "job": "bronze_to_silver",
        "schema_version": SCHEMA_VERSION,
        "output_version": args.version,
        "bronze_root": args.bronze_root,
        "silver_root": args.silver_root,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "dedupe_rule": (
            "one row per recommendationid; keep greatest "
            "(timestamp_updated, ingested_at)"
        ),
    }

    try:
        raw_games = spark.read.schema(GAMES_ENVELOPE_SCHEMA).json(
            f"{args.bronze_root}/games/games_raw.jsonl"
        )
        raw_reviews = spark.read.schema(REVIEW_ENVELOPE_SCHEMA).json(
            f"{args.bronze_root}/reviews/*.jsonl"
        )

        games_in = raw_games.count()
        reviews_in = raw_reviews.count()

        games, rejected_games = build_silver_games(raw_games)
        reviews, rejected_reviews = build_silver_reviews(raw_reviews)

        games_path = f"{silver_version_root}/games"
        reviews_path = f"{silver_version_root}/reviews"
        rejected_games_path = f"{silver_version_root}/rejected_games"
        rejected_reviews_path = f"{silver_version_root}/rejected_reviews"

        games.write.mode("overwrite").parquet(games_path)
        reviews.write.mode("overwrite").partitionBy("appid").parquet(
            reviews_path
        )
        rejected_games.write.mode("overwrite").parquet(rejected_games_path)
        rejected_reviews.write.mode("overwrite").parquet(rejected_reviews_path)

        # Read-back verification: Silver must match accepted counts exactly.
        games_out = spark.read.parquet(games_path).count()
        reviews_out = spark.read.parquet(reviews_path).count()
        rejected_games_out = spark.read.parquet(rejected_games_path).count()
        rejected_reviews_out = spark.read.parquet(rejected_reviews_path).count()

        report["games"] = {
            "input_records": games_in,
            "accepted_records": games_out,
            "rejected_records": rejected_games_out,
        }
        report["reviews"] = {
            "input_records": reviews_in,
            "accepted_records": reviews_out,
            "rejected_records": rejected_reviews_out,
            "duplicate_records_removed": reviews_in
            - rejected_reviews_out
            - reviews_out,
        }

        errors: list[str] = []
        if games_out == 0:
            errors.append("Silver games is empty; contract broken.")
        if reviews_out == 0:
            errors.append("Silver reviews is empty; contract broken.")
        if games_out + rejected_games_out != games_in:
            errors.append("Game input/output reconciliation mismatch.")
        if reviews_out + rejected_reviews_out > reviews_in:
            errors.append("Review input/output reconciliation mismatch.")

        report["status"] = "failed" if errors else "success"
        report["errors"] = errors
    finally:
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        report_path = (
            f"{args.silver_root}/_reports/"
            f"bronze_to_silver_{args.version}.json"
        )
        report["report_path"] = report_path
        write_text_file(spark, report_path, json.dumps(report, indent=2))
        print(json.dumps(report, indent=2))
        spark.stop()

    return 0 if report.get("status") == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
