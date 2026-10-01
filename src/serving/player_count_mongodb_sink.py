"""MongoDB latest-value serving sink for player-count snapshots."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from numbers import Integral
from typing import TYPE_CHECKING, Any, Iterable, Mapping

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


DEFAULT_PLAYER_COUNT_LATEST_COLLECTION = "player_count_latest"


@dataclass(frozen=True)
class PlayerCountMongoConfig:
    uri: str
    database: str
    latest_collection: str

    @classmethod
    def from_environment(cls) -> "PlayerCountMongoConfig":
        return cls(
            uri=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
            database=os.getenv("MONGO_DATABASE", "steam_analytics"),
            latest_collection=os.getenv(
                "MONGO_PLAYER_COUNT_LATEST_COLLECTION",
                DEFAULT_PLAYER_COUNT_LATEST_COLLECTION,
            ),
        )


def _as_mapping(row: Any) -> dict[str, Any]:
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    if isinstance(row, Mapping):
        return dict(row)
    raise TypeError("player-count MongoDB rows must be mappings or Spark Rows")


def _utc_datetime(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError(f"{field} must include a timezone")
        return parsed.astimezone(timezone.utc)
    raise TypeError(f"{field} must be a datetime or ISO-8601 string")


def _utc_datetime_from_epoch_micros(value: Any, field: str) -> datetime:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise TypeError(f"{field} must be an integer epoch value")
    return datetime.fromtimestamp(value / 1_000_000, timezone.utc)


def build_player_count_latest_document(row: Any) -> dict[str, Any]:
    values = _as_mapping(row)
    appid = values.get("appid")
    player_count = values.get("player_count")
    if isinstance(appid, bool) or not isinstance(appid, Integral) or appid <= 0:
        raise ValueError("appid must be a positive integer")
    if (
        isinstance(player_count, bool)
        or not isinstance(player_count, Integral)
        or player_count < 0
    ):
        raise ValueError("player_count must be a non-negative integer")
    event_id = str(values.get("event_id") or "").strip()
    game_name = str(values.get("game_name") or "").strip()
    if not event_id:
        raise ValueError("event_id is required")
    if not game_name:
        raise ValueError("game_name is required")
    observed_at_epoch_micros = values.get("_observed_at_epoch_micros")
    stream_ingested_at_epoch_micros = values.get(
        "_stream_ingested_at_epoch_micros"
    )
    observed_at = (
        _utc_datetime_from_epoch_micros(
            observed_at_epoch_micros,
            "_observed_at_epoch_micros",
        )
        if observed_at_epoch_micros is not None
        else _utc_datetime(values.get("observed_at"), "observed_at")
    )
    stream_ingested_at = (
        _utc_datetime_from_epoch_micros(
            stream_ingested_at_epoch_micros,
            "_stream_ingested_at_epoch_micros",
        )
        if stream_ingested_at_epoch_micros is not None
        else _utc_datetime(
            values.get("stream_ingested_at"),
            "stream_ingested_at",
        )
    )
    return {
        "_id": int(appid),
        "appid": int(appid),
        "game_name": game_name,
        "player_count": int(player_count),
        "observed_at": observed_at,
        "stream_ingested_at": stream_ingested_at,
        "event_id": event_id,
    }


def _upsert_latest_documents(
    config: PlayerCountMongoConfig,
    documents: Iterable[dict[str, Any]],
) -> int:
    from pymongo import MongoClient

    items = list(documents)
    if not items:
        return 0
    client = MongoClient(config.uri, serverSelectionTimeoutMS=5_000)
    try:
        client.admin.command("ping")
        collection = client[config.database][config.latest_collection]
        for item in items:
            identity = item["_id"]
            mutable = {key: value for key, value in item.items() if key != "_id"}
            collection.update_one(
                {
                    "_id": identity,
                    "$or": [
                        {"observed_at": {"$lte": item["observed_at"]}},
                        {"observed_at": {"$exists": False}},
                    ],
                },
                {"$set": mutable},
                upsert=False,
            )
            collection.update_one(
                {"_id": identity},
                {"$setOnInsert": mutable},
                upsert=True,
            )
        return len(items)
    finally:
        client.close()


def write_player_count_latest_batch(
    batch: "DataFrame",
    _batch_id: int,
    config: PlayerCountMongoConfig,
) -> None:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if batch.isEmpty():
        return
    ranking = Window.partitionBy("appid").orderBy(
        F.col("observed_at").desc(),
        F.col("event_id").desc(),
    )
    latest = (
        batch.withColumn("_row_number", F.row_number().over(ranking))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
        .withColumn(
            "_observed_at_epoch_micros",
            F.unix_micros("observed_at"),
        )
        .withColumn(
            "_stream_ingested_at_epoch_micros",
            F.unix_micros("stream_ingested_at"),
        )
    )
    documents = (
        build_player_count_latest_document(row)
        for row in latest.toLocalIterator()
    )
    _upsert_latest_documents(config, documents)


def ensure_player_count_mongodb(config: PlayerCountMongoConfig) -> None:
    from pymongo import MongoClient

    client = MongoClient(config.uri, serverSelectionTimeoutMS=5_000)
    try:
        client.admin.command("ping")
        client[config.database][config.latest_collection].create_index(
            [("observed_at", -1)],
            name="observed_at_desc",
        )
    finally:
        client.close()
