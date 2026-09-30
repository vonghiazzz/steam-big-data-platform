"""Spark Structured Streaming pipeline for REVIEW_CREATED events only."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import partial

from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.common.config import (
    HDFS_BRONZE_STREAM_EVENTS,
    HDFS_GOLD_BASE_INCREMENTAL_V1,
    HDFS_REVIEW_BRONZE_CHECKPOINT_V1,
    HDFS_REVIEW_EVENTS_QUARANTINE,
    HDFS_REVIEW_GOLD_CHECKPOINT_V1,
    HDFS_REVIEW_QUARANTINE_CHECKPOINT_V1,
    HDFS_REVIEW_SILVER_CHECKPOINT_V1,
    HDFS_SILVER_GAMES,
    HDFS_SILVER_REVIEWS,
    HDFS_SILVER_REVIEWS_INCREMENTAL_V1,
    HDFS_STEAM_ROOT,
    PROJECT_ROOT,
)
from src.gold.gold_join import create_gold_dataset
from src.schemas.steam_bronze_schema import REVIEW_DATA_SCHEMA
from src.serving.realtime_mongodb_sink import (
    RealtimeMongoConfig,
    build_realtime_game_metrics,
    ensure_realtime_mongodb,
    prepare_recent_reviews,
    write_realtime_metrics_batch,
    write_recent_reviews_batch,
)
from src.silver.clean_reviews import transform_reviews
from src.streaming.event_contract import REVIEW_CREATED


EVENT_ENVELOPE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("appid", IntegerType(), True),
        StructField("event_time", StringType(), True),
        StructField("produced_at", StringType(), True),
        StructField("payload", REVIEW_DATA_SCHEMA, True),
    ]
)

SILVER_REVIEW_COLUMNS = [
    "recommendationid",
    "appid",
    "game_name",
    "language",
    "voted_up",
    "playtime_at_review",
    "playtime_forever",
    "steam_purchase",
    "received_for_free",
    "timestamp_created",
    "votes_up",
    "votes_funny",
    "weighted_vote_score",
    "recommendation_label",
    "playtime_hours",
    "language_clean",
    "review_year",
]


@dataclass(frozen=True)
class StreamPaths:
    bronze_events: str
    silver_reviews: str
    gold_base: str
    quarantine: str
    bronze_checkpoint: str
    silver_checkpoint: str
    quarantine_checkpoint: str
    gold_checkpoint: str
    mongodb_recent_checkpoint: str
    mongodb_metrics_checkpoint: str


def resolve_stream_paths() -> StreamPaths:
    """Resolve production defaults or an explicitly isolated test root."""
    data_root = os.getenv("STREAM_HDFS_ROOT", "").rstrip("/")
    checkpoint_root = os.getenv("STREAM_CHECKPOINT_ROOT", "").rstrip("/")
    if data_root:
        bronze_events = f"{data_root}/bronze/stream_events"
        silver_reviews = f"{data_root}/silver/reviews_incremental_v1"
        gold_base = f"{data_root}/gold/base_incremental_v1"
        quarantine = f"{data_root}/quarantine/review_events"
    else:
        bronze_events = HDFS_BRONZE_STREAM_EVENTS
        silver_reviews = HDFS_SILVER_REVIEWS_INCREMENTAL_V1
        gold_base = HDFS_GOLD_BASE_INCREMENTAL_V1
        quarantine = HDFS_REVIEW_EVENTS_QUARANTINE

    if checkpoint_root:
        bronze_checkpoint = f"{checkpoint_root}/review_bronze_archive_v1"
        silver_checkpoint = f"{checkpoint_root}/review_silver_v1"
        quarantine_checkpoint = f"{checkpoint_root}/review_quarantine_v1"
        gold_checkpoint = f"{checkpoint_root}/review_gold_v1"
        mongodb_recent_checkpoint = (
            f"{checkpoint_root}/mongodb/recent_reviews"
        )
        mongodb_metrics_checkpoint = (
            f"{checkpoint_root}/mongodb/realtime_game_metrics"
        )
    else:
        bronze_checkpoint = HDFS_REVIEW_BRONZE_CHECKPOINT_V1
        silver_checkpoint = HDFS_REVIEW_SILVER_CHECKPOINT_V1
        quarantine_checkpoint = HDFS_REVIEW_QUARANTINE_CHECKPOINT_V1
        gold_checkpoint = HDFS_REVIEW_GOLD_CHECKPOINT_V1
        mongodb_recent_checkpoint = (
            f"{HDFS_STEAM_ROOT}/checkpoints/mongodb/recent_reviews"
        )
        mongodb_metrics_checkpoint = (
            f"{HDFS_STEAM_ROOT}/checkpoints/mongodb/realtime_game_metrics"
        )

    return StreamPaths(
        bronze_events=bronze_events,
        silver_reviews=silver_reviews,
        gold_base=gold_base,
        quarantine=quarantine,
        bronze_checkpoint=bronze_checkpoint,
        silver_checkpoint=silver_checkpoint,
        quarantine_checkpoint=quarantine_checkpoint,
        gold_checkpoint=gold_checkpoint,
        mongodb_recent_checkpoint=mongodb_recent_checkpoint,
        mongodb_metrics_checkpoint=mongodb_metrics_checkpoint,
    )


def parse_and_classify_events(
    kafka_df: DataFrame,
    games_df: DataFrame,
) -> DataFrame:
    """Parse Kafka values and attach an explicit validation error reason."""
    parsed = (
        kafka_df.select(
            F.col("value").cast("string").alias("raw_event"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn(
            "event",
            F.from_json(F.col("raw_event"), EVENT_ENVELOPE_SCHEMA),
        )
        .select(
            "raw_event",
            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            F.col("event.event_id").alias("event_id"),
            F.col("event.event_type").alias("event_type"),
            F.col("event.appid").alias("appid"),
            F.col("event.event_time").alias("event_time"),
            F.to_timestamp(F.col("event.event_time")).alias("event_time_ts"),
            F.col("event.produced_at").alias("produced_at"),
            F.col("event.payload").alias("payload"),
            F.col("event.payload.recommendationid").alias("recommendationid"),
        )
        .withColumn("ingest_date", F.to_date(F.col("kafka_timestamp")))
        .withColumn("processing_timestamp", F.current_timestamp())
    )

    game_lookup = games_df.select(
        F.col("appid").alias("known_appid"),
        F.col("game_name").alias("known_game_name"),
    )
    enriched = parsed.join(
        F.broadcast(game_lookup),
        parsed.appid == game_lookup.known_appid,
        "left",
    )

    error_items = F.array(
        F.when(F.col("event_id").isNull(), F.lit("MISSING_EVENT_ID")),
        F.when(
            F.col("event_type") != F.lit(REVIEW_CREATED),
            F.lit("UNSUPPORTED_EVENT_TYPE"),
        ),
        F.when(
            F.col("appid").isNull() | (F.col("appid") <= F.lit(0)),
            F.lit("INVALID_APPID"),
        ),
        F.when(F.col("event_time_ts").isNull(), F.lit("INVALID_EVENT_TIME")),
        F.when(F.col("produced_at").isNull(), F.lit("MISSING_PRODUCED_AT")),
        F.when(F.col("payload").isNull(), F.lit("MISSING_PAYLOAD")),
        F.when(
            F.col("payload.recommendationid").isNull()
            | (F.length(F.trim(F.col("payload.recommendationid"))) == 0),
            F.lit("INVALID_RECOMMENDATIONID"),
        ),
        F.when(
            F.col("payload.timestamp_created").isNull(),
            F.lit("MISSING_TIMESTAMP_CREATED"),
        ),
        F.when(F.col("payload.voted_up").isNull(), F.lit("MISSING_VOTED_UP")),
        F.when(F.col("known_appid").isNull(), F.lit("UNKNOWN_APPID")),
    )
    return enriched.withColumn(
        "error_reason",
        F.concat_ws(
            ";",
            F.filter(error_items, lambda value: value.isNotNull()),
        ),
    )


def build_silver_reviews(valid_events: DataFrame) -> DataFrame:
    """Reuse the batch review transformation and retain stream metadata."""
    bronze_like = valid_events.select(
        "appid",
        F.col("known_game_name").alias("game_name"),
        F.col("payload").alias("review"),
    )
    # Deduplication already happened on the watermarked event stream. Avoid a
    # stream-stream self-join merely to retain a partition date: it creates an
    # unnecessary second state store and hundreds of checkpoint files.
    return transform_reviews(bronze_like).withColumn(
        "ingest_date",
        F.to_date(F.from_unixtime(F.col("timestamp_created"))),
    )


def remove_batch_baseline_ids(
    silver_stream: DataFrame,
    baseline_reviews: DataFrame,
) -> DataFrame:
    """Defense in depth: canonical 25k IDs cannot enter stream Silver."""
    known_ids = baseline_reviews.select(
        F.trim(F.col("recommendationid")).alias("known_recommendationid")
    ).dropDuplicates()
    return (
        silver_stream.join(
            F.broadcast(known_ids),
            F.col("recommendationid") == F.col("known_recommendationid"),
            "left",
        )
        .filter(F.col("known_recommendationid").isNull())
        .drop("known_recommendationid")
    )


def remove_existing_gold_ids(
    spark: SparkSession,
    gold_stream: DataFrame,
    gold_path: str,
) -> DataFrame:
    """Prevent a fresh file-source checkpoint from replaying existing Gold."""
    try:
        existing_ids = (
            spark.read.option("basePath", gold_path)
            .parquet(f"{gold_path}/review_date=*/*.parquet")
            .select(
                F.trim(F.col("recommendationid")).alias(
                    "existing_recommendationid"
                )
            )
            .dropDuplicates()
        )
    except AnalysisException as exc:
        if (
            "UNABLE_TO_INFER_SCHEMA" not in str(exc)
            and "PATH_NOT_FOUND" not in str(exc)
        ):
            raise
        existing_ids = spark.createDataFrame(
            [],
            "existing_recommendationid string",
        )
    return (
        gold_stream.join(
            F.broadcast(existing_ids),
            F.col("recommendationid") == F.col("existing_recommendationid"),
            "left",
        )
        .filter(F.col("existing_recommendationid").isNull())
        .drop("existing_recommendationid")
    )


def archive_projection(classified: DataFrame) -> DataFrame:
    return classified.select(
        "event_id",
        "event_type",
        "appid",
        "event_time",
        "produced_at",
        F.to_json(F.col("payload")).alias("payload"),
        "raw_event",
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "ingest_date",
    )


def quarantine_projection(classified: DataFrame) -> DataFrame:
    return classified.filter(F.col("error_reason") != "").select(
        "raw_event",
        "error_reason",
        "event_id",
        "appid",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "processing_timestamp",
        "ingest_date",
    )


def _ensure_path(spark: SparkSession, path: str) -> None:
    jvm = spark._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    filesystem.mkdirs(hadoop_path)


def _file_sink(
    frame: DataFrame,
    *,
    path: str,
    checkpoint: str,
    trigger_interval: str,
    partition_column: str,
    available_now: bool,
):
    # One local output task per non-empty micro-batch bounds file creation.
    writer = (
        frame.repartition(1)
        .writeStream.format("parquet")
        .outputMode("append")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .option("compression", "snappy")
        .partitionBy(partition_column)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=trigger_interval)
    return writer.start()


def _gold_sink(
    frame: DataFrame,
    *,
    path: str,
    checkpoint: str,
    trigger_interval: str,
    available_now: bool,
):
    """Write idempotent Gold batches without a conflicting file-sink log."""

    def write_batch(batch: DataFrame, _batch_id: int) -> None:
        if batch.isEmpty():
            return
        new_rows = remove_existing_gold_ids(batch.sparkSession, batch, path)
        if new_rows.isEmpty():
            return
        (
            new_rows.repartition(1)
            .write.mode("append")
            .option("compression", "snappy")
            .partitionBy("review_date")
            .parquet(path)
        )

    writer = frame.writeStream.foreachBatch(write_batch).option(
        "checkpointLocation",
        checkpoint,
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=trigger_interval)
    return writer.start()


def _mongodb_sink(
    frame: DataFrame,
    *,
    callback,
    checkpoint: str,
    trigger_interval: str,
    available_now: bool,
    output_mode: str,
):
    writer = (
        frame.writeStream.outputMode(output_mode)
        .foreachBatch(callback)
        .option("checkpointLocation", checkpoint)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=trigger_interval)
    return writer.start()


def _start_mongodb_queries(
    incremental_reviews: DataFrame,
    *,
    config: RealtimeMongoConfig,
    paths: StreamPaths,
    watermark_delay: str,
    trigger_interval: str,
    available_now: bool,
) -> list:
    recent_reviews = prepare_recent_reviews(incremental_reviews)
    realtime_metrics = build_realtime_game_metrics(
        recent_reviews,
        watermark_delay,
    )
    recent_query = _mongodb_sink(
        recent_reviews.drop("event_time_ts"),
        callback=partial(write_recent_reviews_batch, config=config),
        checkpoint=paths.mongodb_recent_checkpoint,
        trigger_interval=trigger_interval,
        available_now=available_now,
        output_mode="append",
    )
    metrics_query = _mongodb_sink(
        realtime_metrics,
        callback=partial(write_realtime_metrics_batch, config=config),
        checkpoint=paths.mongodb_metrics_checkpoint,
        trigger_interval=trigger_interval,
        available_now=available_now,
        output_mode="update",
    )
    return [recent_query, metrics_query]


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env", override=False)
    paths = resolve_stream_paths()
    hdfs_default_fs = os.getenv(
        "HDFS_DEFAULT_FS",
        "hdfs://bda501-namenode.orb.local:8020",
    )
    spark = (
        SparkSession.builder.appName("Steam REVIEW_CREATED Streaming V1")
        .config("spark.hadoop.fs.defaultFS", hdfs_default_fs)
        .config(
            "spark.hadoop.dfs.replication",
            os.getenv("STREAM_HDFS_REPLICATION", "1"),
        )
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv("STREAM_SPARK_SHUFFLE_PARTITIONS", "3"),
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )
    topic = os.getenv("KAFKA_TOPIC", "steam_events")
    trigger_interval = os.getenv(
        "STREAM_SPARK_TRIGGER_INTERVAL",
        "1 minute",
    )
    watermark_delay = os.getenv("STREAM_WATERMARK_DELAY", "7 days")
    max_offsets = os.getenv("STREAM_MAX_OFFSETS_PER_TRIGGER", "1000")
    available_now = os.getenv("STREAM_AVAILABLE_NOW", "false").lower() in {
        "1",
        "true",
        "yes",
    }
    mongodb_realtime_enabled = os.getenv(
        "MONGO_REALTIME_ENABLED",
        "false",
    ).lower() in {"1", "true", "yes"}
    mongodb_config = None
    if mongodb_realtime_enabled:
        mongodb_config = RealtimeMongoConfig.from_environment()
        ensure_realtime_mongodb(mongodb_config)

    output_paths = [
        paths.bronze_events,
        paths.silver_reviews,
        paths.gold_base,
        paths.quarantine,
    ]
    for path in output_paths:
        _ensure_path(spark, path)

    games = spark.read.parquet(HDFS_SILVER_GAMES).cache()
    baseline_reviews = spark.read.parquet(HDFS_SILVER_REVIEWS).select(
        "recommendationid"
    )

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )
    classified = parse_and_classify_events(kafka_stream, games)

    archive_query = _file_sink(
        archive_projection(classified),
        path=paths.bronze_events,
        checkpoint=paths.bronze_checkpoint,
        trigger_interval=trigger_interval,
        partition_column="ingest_date",
        available_now=available_now,
    )
    quarantine_query = _file_sink(
        quarantine_projection(classified),
        path=paths.quarantine,
        checkpoint=paths.quarantine_checkpoint,
        trigger_interval=trigger_interval,
        partition_column="ingest_date",
        available_now=available_now,
    )

    valid = classified.filter(F.col("error_reason") == "")
    deduplicated = valid.withWatermark(
        "event_time_ts",
        watermark_delay,
    ).dropDuplicatesWithinWatermark(["recommendationid"])
    silver = remove_batch_baseline_ids(
        build_silver_reviews(deduplicated),
        baseline_reviews,
    ).select(*SILVER_REVIEW_COLUMNS, "ingest_date")
    silver_query = _file_sink(
        silver,
        path=paths.silver_reviews,
        checkpoint=paths.silver_checkpoint,
        trigger_interval=trigger_interval,
        partition_column="ingest_date",
        available_now=available_now,
    )

    incremental_schema = silver.select(
        *SILVER_REVIEW_COLUMNS,
        "ingest_date",
    ).schema
    incremental_reviews = (
        spark.readStream.schema(incremental_schema)
        .option("maxFilesPerTrigger", "10")
        .parquet(paths.silver_reviews)
    )
    gold = remove_existing_gold_ids(
        spark,
        create_gold_dataset(incremental_reviews, games).withColumn(
            "review_date",
            F.to_date(F.from_unixtime(F.col("timestamp_created"))),
        ),
        paths.gold_base,
    )
    source_queries = [archive_query, quarantine_query, silver_query]
    queries = list(source_queries)
    try:
        if available_now:
            # Gold must snapshot the incremental file source only after the
            # bounded Silver query has committed its files.
            for query in source_queries:
                query.awaitTermination()
            gold_query = _gold_sink(
                gold,
                path=paths.gold_base,
                checkpoint=paths.gold_checkpoint,
                trigger_interval=trigger_interval,
                available_now=True,
            )
            queries.append(gold_query)
            if mongodb_config is not None:
                queries.extend(
                    _start_mongodb_queries(
                        incremental_reviews,
                        config=mongodb_config,
                        paths=paths,
                        watermark_delay=watermark_delay,
                        trigger_interval=trigger_interval,
                        available_now=True,
                    )
                )
            for query in queries[len(source_queries) :]:
                query.awaitTermination()
        else:
            gold_query = _gold_sink(
                gold,
                path=paths.gold_base,
                checkpoint=paths.gold_checkpoint,
                trigger_interval=trigger_interval,
                available_now=False,
            )
            queries.append(gold_query)
            if mongodb_config is not None:
                queries.extend(
                    _start_mongodb_queries(
                        incremental_reviews,
                        config=mongodb_config,
                        paths=paths,
                        watermark_delay=watermark_delay,
                        trigger_interval=trigger_interval,
                        available_now=False,
                    )
                )
            spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
