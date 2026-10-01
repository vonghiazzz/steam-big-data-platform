"""Spark Structured Streaming pipeline for PLAYER_COUNT_SNAPSHOT events."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import partial

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.common.config import (
    HDFS_BRONZE_PLAYER_COUNT_EVENTS,
    HDFS_GOLD_PLAYER_COUNT_SNAPSHOTS_V1,
    HDFS_PLAYER_COUNT_BRONZE_CHECKPOINT_V1,
    HDFS_PLAYER_COUNT_EVENTS_QUARANTINE,
    HDFS_PLAYER_COUNT_GOLD_CHECKPOINT_V1,
    HDFS_PLAYER_COUNT_QUARANTINE_CHECKPOINT_V1,
    HDFS_PLAYER_COUNT_SILVER_CHECKPOINT_V1,
    HDFS_SILVER_GAMES,
    HDFS_SILVER_PLAYER_COUNT_SNAPSHOTS_V1,
    HDFS_STEAM_ROOT,
    PROJECT_ROOT,
)
from src.serving.player_count_mongodb_sink import (
    PlayerCountMongoConfig,
    ensure_player_count_mongodb,
    write_player_count_latest_batch,
)
from src.streaming.event_contract import (
    PLAYER_COUNT_SCHEMA_VERSION,
    PLAYER_COUNT_SNAPSHOT,
)


PLAYER_COUNT_PAYLOAD_SCHEMA = StructType(
    [StructField("player_count", LongType(), True)]
)

PLAYER_COUNT_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("schema_version", IntegerType(), True),
        StructField("appid", IntegerType(), True),
        StructField("event_time", StringType(), True),
        StructField("produced_at", StringType(), True),
        StructField("payload", PLAYER_COUNT_PAYLOAD_SCHEMA, True),
    ]
)


@dataclass(frozen=True)
class PlayerCountStreamPaths:
    bronze_events: str
    silver_snapshots: str
    gold_snapshots: str
    quarantine: str
    bronze_checkpoint: str
    silver_checkpoint: str
    gold_checkpoint: str
    quarantine_checkpoint: str
    mongodb_checkpoint: str


def resolve_player_count_paths() -> PlayerCountStreamPaths:
    data_root = os.getenv("PLAYER_COUNT_HDFS_ROOT", "").rstrip("/")
    checkpoint_root = os.getenv(
        "PLAYER_COUNT_CHECKPOINT_ROOT",
        "",
    ).rstrip("/")
    if data_root:
        bronze_events = f"{data_root}/bronze/player_count_events"
        silver_snapshots = f"{data_root}/silver/player_count_snapshots_v1"
        gold_snapshots = f"{data_root}/gold/player_count_snapshots_v1"
        quarantine = f"{data_root}/quarantine/player_count_events"
    else:
        bronze_events = HDFS_BRONZE_PLAYER_COUNT_EVENTS
        silver_snapshots = HDFS_SILVER_PLAYER_COUNT_SNAPSHOTS_V1
        gold_snapshots = HDFS_GOLD_PLAYER_COUNT_SNAPSHOTS_V1
        quarantine = HDFS_PLAYER_COUNT_EVENTS_QUARANTINE

    if checkpoint_root:
        bronze_checkpoint = f"{checkpoint_root}/player_count_bronze_v1"
        silver_checkpoint = f"{checkpoint_root}/player_count_silver_v1"
        gold_checkpoint = f"{checkpoint_root}/player_count_gold_v1"
        quarantine_checkpoint = (
            f"{checkpoint_root}/player_count_quarantine_v1"
        )
        mongodb_checkpoint = (
            f"{checkpoint_root}/mongodb/player_count_latest_v1"
        )
    else:
        bronze_checkpoint = HDFS_PLAYER_COUNT_BRONZE_CHECKPOINT_V1
        silver_checkpoint = HDFS_PLAYER_COUNT_SILVER_CHECKPOINT_V1
        gold_checkpoint = HDFS_PLAYER_COUNT_GOLD_CHECKPOINT_V1
        quarantine_checkpoint = HDFS_PLAYER_COUNT_QUARANTINE_CHECKPOINT_V1
        mongodb_checkpoint = (
            f"{HDFS_STEAM_ROOT}/checkpoints/mongodb/player_count_latest_v1"
        )

    return PlayerCountStreamPaths(
        bronze_events=bronze_events,
        silver_snapshots=silver_snapshots,
        gold_snapshots=gold_snapshots,
        quarantine=quarantine,
        bronze_checkpoint=bronze_checkpoint,
        silver_checkpoint=silver_checkpoint,
        gold_checkpoint=gold_checkpoint,
        quarantine_checkpoint=quarantine_checkpoint,
        mongodb_checkpoint=mongodb_checkpoint,
    )


def parse_and_classify_player_count_events(
    kafka_df: DataFrame,
    games_df: DataFrame,
) -> DataFrame:
    raw = kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_event"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).withColumn(
        "event",
        F.from_json(F.col("raw_event"), PLAYER_COUNT_EVENT_SCHEMA),
    )
    parsed = raw.select(
        "raw_event",
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.col("event.event_id").alias("event_id"),
        F.col("event.event_type").alias("event_type"),
        F.col("event.schema_version").alias("schema_version"),
        F.col("event.appid").alias("appid"),
        F.col("event.event_time").alias("event_time"),
        F.col("event.produced_at").alias("produced_at"),
        F.col("event.payload.player_count").alias("player_count"),
    ).withColumns(
        {
            "event_time_ts": F.to_timestamp("event_time"),
            "produced_at_ts": F.to_timestamp("produced_at"),
        }
    )
    games = games_df.select(
        F.col("appid").alias("known_appid"),
        F.col("game_name"),
    )
    joined = parsed.join(
        F.broadcast(games),
        parsed.appid == games.known_appid,
        "left",
    )
    error_reason = (
        F.when(F.col("event_id").isNull(), "MISSING_EVENT_ID")
        .when(
            F.col("event_type").isNull()
            | (F.col("event_type") != F.lit(PLAYER_COUNT_SNAPSHOT)),
            "UNSUPPORTED_EVENT_TYPE",
        )
        .when(
            F.col("schema_version").isNull()
            | (
                F.col("schema_version")
                != F.lit(PLAYER_COUNT_SCHEMA_VERSION)
            ),
            "UNSUPPORTED_SCHEMA_VERSION",
        )
        .when(F.col("appid").isNull() | (F.col("appid") <= 0), "INVALID_APPID")
        .when(F.col("known_appid").isNull(), "UNKNOWN_APPID")
        .when(
            F.col("player_count").isNull() | (F.col("player_count") < 0),
            "INVALID_PLAYER_COUNT",
        )
        .when(F.col("event_time_ts").isNull(), "INVALID_EVENT_TIME")
        .when(F.col("produced_at_ts").isNull(), "INVALID_PRODUCED_AT")
        .when(
            F.col("kafka_key").isNotNull()
            & (F.trim(F.col("kafka_key")) != F.col("appid").cast("string")),
            "KAFKA_KEY_APPID_MISMATCH",
        )
        .otherwise("")
    )
    return (
        joined.withColumn("error_reason", error_reason)
        .withColumn("processing_timestamp", F.current_timestamp())
        .withColumn("ingest_date", F.to_date(F.current_timestamp()))
        .drop("known_appid")
    )


def player_count_archive_projection(classified: DataFrame) -> DataFrame:
    return classified.select(
        "event_id",
        "event_type",
        "schema_version",
        "appid",
        "event_time",
        "produced_at",
        "player_count",
        "raw_event",
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "processing_timestamp",
        "ingest_date",
    )


def player_count_quarantine_projection(classified: DataFrame) -> DataFrame:
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


def build_player_count_silver(
    classified: DataFrame,
    watermark_delay: str,
) -> DataFrame:
    valid = classified.filter(F.col("error_reason") == "")
    deduplicated = valid.withWatermark(
        "event_time_ts",
        watermark_delay,
    ).dropDuplicatesWithinWatermark(["event_id"])
    return deduplicated.select(
        "event_id",
        "appid",
        "game_name",
        F.col("player_count").cast("long").alias("player_count"),
        F.col("event_time_ts").alias("observed_at"),
        F.col("produced_at_ts").alias("produced_at"),
        F.current_timestamp().alias("stream_ingested_at"),
        F.to_date(F.col("event_time_ts")).alias("snapshot_date"),
    )


def build_player_count_gold(silver: DataFrame) -> DataFrame:
    return silver.select(
        "event_id",
        "appid",
        "game_name",
        "player_count",
        "observed_at",
        "produced_at",
        "stream_ingested_at",
        "snapshot_date",
        F.date_trunc("hour", F.col("observed_at")).alias("snapshot_hour"),
    )


def _ensure_path(spark: SparkSession, path: str) -> None:
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
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


def _mongodb_sink(
    frame: DataFrame,
    *,
    config: PlayerCountMongoConfig,
    checkpoint: str,
    trigger_interval: str,
    available_now: bool,
):
    writer = (
        frame.writeStream.outputMode("append")
        .foreachBatch(partial(write_player_count_latest_batch, config=config))
        .option("checkpointLocation", checkpoint)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=trigger_interval)
    return writer.start()


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env", override=False)
    paths = resolve_player_count_paths()
    hdfs_default_fs = os.getenv(
        "HDFS_DEFAULT_FS",
        "hdfs://bda501-namenode.orb.local:8020",
    )
    spark = (
        SparkSession.builder.appName("Steam PLAYER_COUNT_SNAPSHOT Streaming V1")
        .config("spark.hadoop.fs.defaultFS", hdfs_default_fs)
        .config(
            "spark.hadoop.dfs.replication",
            os.getenv("STREAM_HDFS_REPLICATION", "1"),
        )
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv("STREAM_SPARK_SHUFFLE_PARTITIONS", "3"),
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    trigger_interval = os.getenv(
        "PLAYER_COUNT_SPARK_TRIGGER_INTERVAL",
        os.getenv("STREAM_SPARK_TRIGGER_INTERVAL", "1 minute"),
    )
    watermark_delay = os.getenv("PLAYER_COUNT_WATERMARK_DELAY", "1 day")
    available_now = os.getenv(
        "PLAYER_COUNT_AVAILABLE_NOW",
        os.getenv("STREAM_AVAILABLE_NOW", "false"),
    ).lower() in {"1", "true", "yes"}
    mongodb_enabled = os.getenv(
        "MONGO_PLAYER_COUNT_ENABLED",
        "false",
    ).lower() in {"1", "true", "yes"}
    mongodb_config = None
    if mongodb_enabled:
        mongodb_config = PlayerCountMongoConfig.from_environment()
        ensure_player_count_mongodb(mongodb_config)

    for path in (
        paths.bronze_events,
        paths.silver_snapshots,
        paths.gold_snapshots,
        paths.quarantine,
    ):
        _ensure_path(spark, path)

    games = spark.read.parquet(HDFS_SILVER_GAMES).cache()
    kafka_stream = (
        spark.readStream.format("kafka")
        .option(
            "kafka.bootstrap.servers",
            os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        )
        .option(
            "subscribe",
            os.getenv("KAFKA_PLAYER_COUNT_TOPIC", "steam_player_events"),
        )
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option(
            "maxOffsetsPerTrigger",
            os.getenv("PLAYER_COUNT_MAX_OFFSETS_PER_TRIGGER", "1000"),
        )
        .load()
    )
    classified = parse_and_classify_player_count_events(kafka_stream, games)
    silver = build_player_count_silver(classified, watermark_delay)
    gold = build_player_count_gold(silver)

    queries = [
        _file_sink(
            player_count_archive_projection(classified),
            path=paths.bronze_events,
            checkpoint=paths.bronze_checkpoint,
            trigger_interval=trigger_interval,
            partition_column="ingest_date",
            available_now=available_now,
        ),
        _file_sink(
            player_count_quarantine_projection(classified),
            path=paths.quarantine,
            checkpoint=paths.quarantine_checkpoint,
            trigger_interval=trigger_interval,
            partition_column="ingest_date",
            available_now=available_now,
        ),
        _file_sink(
            silver,
            path=paths.silver_snapshots,
            checkpoint=paths.silver_checkpoint,
            trigger_interval=trigger_interval,
            partition_column="snapshot_date",
            available_now=available_now,
        ),
        _file_sink(
            gold,
            path=paths.gold_snapshots,
            checkpoint=paths.gold_checkpoint,
            trigger_interval=trigger_interval,
            partition_column="snapshot_date",
            available_now=available_now,
        ),
    ]
    if mongodb_config is not None:
        queries.append(
            _mongodb_sink(
                silver.drop("snapshot_date"),
                config=mongodb_config,
                checkpoint=paths.mongodb_checkpoint,
                trigger_interval=trigger_interval,
                available_now=available_now,
            )
        )

    try:
        if available_now:
            for query in queries:
                query.awaitTermination()
        else:
            spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
