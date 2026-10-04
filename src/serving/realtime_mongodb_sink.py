"""Idempotent MongoDB materialized views for new review streaming data."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from numbers import Integral
from typing import Any, Iterable, Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


DEFAULT_RECENT_REVIEWS_COLLECTION = "recent_reviews"
DEFAULT_REALTIME_METRICS_COLLECTION = "realtime_game_metrics"


@dataclass(frozen=True)
class RealtimeMongoConfig:
    uri: str
    database: str
    recent_reviews_collection: str
    realtime_metrics_collection: str

    @classmethod
    def from_environment(cls) -> "RealtimeMongoConfig":
        return cls(
            uri=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
            database=os.getenv("MONGO_DATABASE", "steam_analytics"),
            recent_reviews_collection=os.getenv(
                "MONGO_RECENT_REVIEWS_COLLECTION",
                DEFAULT_RECENT_REVIEWS_COLLECTION,
            ),
            realtime_metrics_collection=os.getenv(
                "MONGO_REALTIME_METRICS_COLLECTION",
                DEFAULT_REALTIME_METRICS_COLLECTION,
            ),
        )


def _as_mapping(row: Any) -> dict[str, Any]:
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    if isinstance(row, Mapping):
        return dict(row)
    raise TypeError("MongoDB streaming rows must be mappings or Spark Rows")


def _required_text(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    return text


def _integer_or_none(value: Any, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise TypeError(f"{field} must be an integer or null")
    return int(value)


def _boolean_or_none(value: Any, field: str) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise TypeError(f"{field} must be a boolean or null")
    return value


def _utc_datetime(value: Any, field: str) -> datetime:
    if isinstance(value, bool):
        raise TypeError(f"{field} must be a timestamp")
    if isinstance(value, Integral):
        return datetime.fromtimestamp(int(value), timezone.utc)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    raise TypeError(f"{field} must be a datetime, epoch, or ISO-8601 string")


def recent_review_id(row: Mapping[str, Any]) -> str:
    return _required_text(row.get("recommendationid"), "recommendationid")


def realtime_metric_id(
    appid: int,
    window_start: datetime,
    window_end: datetime,
) -> str:
    start = _utc_datetime(window_start, "window_start")
    end = _utc_datetime(window_end, "window_end")
    if end <= start:
        raise ValueError("window_end must be later than window_start")
    start_text = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_text = end.strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"{int(appid)}:{start_text}:{end_text}"


def build_recent_review_document(row: Any) -> dict[str, Any]:
    values = _as_mapping(row)
    identity = recent_review_id(values)
    appid = _integer_or_none(values.get("appid"), "appid")
    if appid is None or appid <= 0:
        raise ValueError("appid must be a positive integer")
    document = {
        "_id": identity,
        "recommendationid": identity,
        "appid": appid,
        "voted_up": _boolean_or_none(values.get("voted_up"), "voted_up"),
        "playtime_at_review": _integer_or_none(
            values.get("playtime_at_review"),
            "playtime_at_review",
        ),
        "playtime_forever": _integer_or_none(
            values.get("playtime_forever"),
            "playtime_forever",
        ),
        "steam_purchase": _boolean_or_none(
            values.get("steam_purchase"),
            "steam_purchase",
        ),
        "received_for_free": _boolean_or_none(
            values.get("received_for_free"),
            "received_for_free",
        ),
        "timestamp_created": _utc_datetime(
            values.get("timestamp_created"),
            "timestamp_created",
        ),
        "stream_ingested_at": _utc_datetime(
            values.get("stream_ingested_at"),
            "stream_ingested_at",
        ),
    }
    game_name = str(values.get("game_name") or "").strip()
    if game_name:
        document["game_name"] = game_name
    for field in ("game_type", "playtime_bucket"):
        value = values.get(field)
        if value is not None:
            document[field] = str(value)
    genres = values.get("genres")
    if genres is not None:
        if not isinstance(genres, (list, tuple)):
            raise TypeError("genres must be a list or tuple")
        document["genres"] = sorted(
            {str(genre).strip() for genre in genres if str(genre).strip()}
        )
    playtime_hours = values.get("playtime_hours")
    if playtime_hours is not None:
        if isinstance(playtime_hours, bool) or not isinstance(
            playtime_hours, (int, float)
        ):
            raise TypeError("playtime_hours must be numeric or null")
        document["playtime_hours"] = float(playtime_hours)
    return document


def build_realtime_metric_document(row: Any) -> dict[str, Any]:
    values = _as_mapping(row)
    appid = _integer_or_none(values.get("appid"), "appid")
    review_count = _integer_or_none(values.get("review_count"), "review_count")
    positive = _integer_or_none(
        values.get("positive_reviews"),
        "positive_reviews",
    )
    negative = _integer_or_none(
        values.get("negative_reviews"),
        "negative_reviews",
    )
    if appid is None or appid <= 0:
        raise ValueError("appid must be a positive integer")
    if review_count is None or review_count <= 0:
        raise ValueError("review_count must be positive")
    if positive is None or negative is None:
        raise ValueError("positive_reviews and negative_reviews are required")
    if positive + negative != review_count:
        raise ValueError(
            "positive_reviews + negative_reviews must equal review_count"
        )

    window_start = _utc_datetime(values.get("window_start"), "window_start")
    window_end = _utc_datetime(values.get("window_end"), "window_end")
    return {
        "_id": realtime_metric_id(appid, window_start, window_end),
        "appid": appid,
        "window_start": window_start,
        "window_end": window_end,
        "review_count": review_count,
        "positive_reviews": positive,
        "negative_reviews": negative,
        "recommendation_rate": float(positive / review_count),
    }


def prepare_recent_reviews(streaming_silver: DataFrame) -> DataFrame:
    """Project the validated incremental Silver stream for MongoDB serving."""
    event_time = F.timestamp_seconds(F.col("timestamp_created"))
    review_columns: list[Any] = [
        "recommendationid",
        "appid",
        "voted_up",
        "playtime_at_review",
        "playtime_forever",
        "steam_purchase",
        "received_for_free",
    ]
    available_columns = set(streaming_silver.columns)
    for column in ("game_name", "genres", "playtime_hours", "playtime_bucket"):
        if column in available_columns:
            review_columns.append(column)
    if "is_free" in available_columns:
        review_columns.append(
            F.when(F.col("is_free") == F.lit(True), F.lit("FREE"))
            .when(F.col("is_free") == F.lit(False), F.lit("PAID"))
            .otherwise(F.lit("UNKNOWN"))
            .alias("game_type")
        )
    elif "game_type" in available_columns:
        review_columns.append("game_type")
    if "playtime_bucket" not in available_columns:
        hours = F.col("playtime_at_review").cast("double") / F.lit(60.0)
        review_columns.append(
            F.when(
                hours.isNull() | (hours < F.lit(0.0)),
                F.lit(None).cast("string"),
            )
            .when(hours < F.lit(2.0), F.lit("0-2h"))
            .when(hours < F.lit(10.0), F.lit("2-10h"))
            .when(hours < F.lit(50.0), F.lit("10-50h"))
            .otherwise(F.lit("50h+"))
            .alias("playtime_bucket")
        )
    if "playtime_hours" not in available_columns:
        review_columns.append(
            (
                F.col("playtime_at_review").cast("double") / F.lit(60.0)
            ).alias("playtime_hours")
        )
    return streaming_silver.select(
        *review_columns,
        event_time.alias("timestamp_created"),
        event_time.alias("event_time_ts"),
        F.current_timestamp().alias("stream_ingested_at"),
    )


def build_realtime_game_metrics(
    recent_reviews: DataFrame,
    watermark_delay: str,
) -> DataFrame:
    """Build a stateful one-hour event-time aggregation for review metrics."""
    return (
        recent_reviews.withWatermark("event_time_ts", watermark_delay)
        .groupBy(
            "appid",
            F.window(F.col("event_time_ts"), "1 hour").alias("review_window"),
        )
        .agg(
            F.count(F.lit(1)).cast("long").alias("review_count"),
            F.sum(
                F.when(F.col("voted_up") == F.lit(True), 1).otherwise(0)
            )
            .cast("long")
            .alias("positive_reviews"),
            F.sum(
                F.when(F.col("voted_up") == F.lit(False), 1).otherwise(0)
            )
            .cast("long")
            .alias("negative_reviews"),
        )
        .select(
            "appid",
            F.col("review_window.start").alias("window_start"),
            F.col("review_window.end").alias("window_end"),
            "review_count",
            "positive_reviews",
            "negative_reviews",
            (
                F.col("positive_reviews").cast("double")
                / F.col("review_count")
            ).alias("recommendation_rate"),
        )
    )


def _upsert_documents(
    config: RealtimeMongoConfig,
    collection_name: str,
    documents: Iterable[dict[str, Any]],
) -> int:
    from pymongo import MongoClient, ReplaceOne

    items = list(documents)
    if not items:
        return 0
    client = MongoClient(config.uri, serverSelectionTimeoutMS=5_000)
    try:
        client.admin.command("ping")
        operations = [
            ReplaceOne({"_id": item["_id"]}, item, upsert=True)
            for item in items
        ]
        client[config.database][collection_name].bulk_write(
            operations,
            ordered=False,
        )
        return len(items)
    finally:
        client.close()


def write_recent_reviews_batch(
    batch: DataFrame,
    _batch_id: int,
    config: RealtimeMongoConfig,
) -> None:
    documents = (
        build_recent_review_document(row) for row in batch.toLocalIterator()
    )
    _upsert_documents(config, config.recent_reviews_collection, documents)


def write_realtime_metrics_batch(
    batch: DataFrame,
    _batch_id: int,
    config: RealtimeMongoConfig,
) -> None:
    documents = (
        build_realtime_metric_document(row) for row in batch.toLocalIterator()
    )
    _upsert_documents(config, config.realtime_metrics_collection, documents)


def ensure_realtime_mongodb(config: RealtimeMongoConfig) -> None:
    """Fail fast on connectivity and create only the two V2 indexes."""
    from pymongo import MongoClient

    client = MongoClient(config.uri, serverSelectionTimeoutMS=5_000)
    try:
        client.admin.command("ping")
        database = client[config.database]
        database[config.recent_reviews_collection].create_index(
            [("appid", 1), ("timestamp_created", -1)],
            name="appid_timestamp_created_desc",
        )
        database[config.realtime_metrics_collection].create_index(
            [("appid", 1), ("window_start", -1)],
            name="appid_window_start_desc",
        )
    finally:
        client.close()
