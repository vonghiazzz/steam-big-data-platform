"""Collect per-game Bronze, Silver, Gold, and Mongo readiness evidence."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import (
    HDFS_BRONZE_GAMES,
    HDFS_BRONZE_REVIEWS,
    HDFS_GOLD_BASE,
    HDFS_SILVER_GAMES,
    HDFS_SILVER_REVIEWS,
)
from src.onboarding.manifest import load_manifest
from src.schemas.steam_bronze_schema import (
    STEAM_GAMES_BRONZE_SCHEMA,
    STEAM_REVIEWS_BRONZE_SCHEMA,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    )
    parser.add_argument(
        "--mongo-database",
        default=os.getenv("MONGO_DATABASE", "steam_analytics"),
    )
    return parser.parse_args()


def _counts(frame, appids: tuple[int, ...]) -> dict[int, int]:
    return {
        int(row["appid"]): int(row["count"])
        for row in (
            frame.filter(F.col("appid").isin(list(appids)))
            .groupBy("appid")
            .count()
            .collect()
        )
    }


def main() -> int:
    from pymongo import MongoClient

    args = parse_args()
    manifest = load_manifest(args.manifest)
    appids = manifest.appids
    spark = SparkSession.builder.appName("SteamOnboardingReadiness").getOrCreate()
    client = None
    try:
        bronze_games = spark.read.schema(STEAM_GAMES_BRONZE_SCHEMA).json(
            HDFS_BRONZE_GAMES
        )
        bronze_reviews = spark.read.schema(STEAM_REVIEWS_BRONZE_SCHEMA).json(
            HDFS_BRONZE_REVIEWS
        )
        silver_games = spark.read.parquet(HDFS_SILVER_GAMES)
        silver_reviews = spark.read.parquet(HDFS_SILVER_REVIEWS)
        gold = spark.read.parquet(HDFS_GOLD_BASE)

        bronze_game_ids = {
            int(row["appid"])
            for row in bronze_games.filter(F.col("appid").isin(list(appids)))
            .select("appid")
            .distinct()
            .collect()
        }
        bronze_review_counts = _counts(bronze_reviews, appids)
        silver_game_ids = {
            int(row["appid"])
            for row in silver_games.filter(F.col("appid").isin(list(appids)))
            .select("appid")
            .distinct()
            .collect()
        }
        silver_review_counts = _counts(silver_reviews, appids)
        gold_review_counts = _counts(gold, appids)

        client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5_000)
        client.admin.command("ping")
        mongo_ids = {
            int(row["_id"])
            for row in client[args.mongo_database]["game_metrics"].find(
                {"_id": {"$in": list(appids)}}, {"_id": 1}
            )
        }

        games: dict[str, dict] = {}
        for game in manifest.games:
            expected = game.target_reviews
            state = {
                "appid": game.appid,
                "bronze_metadata": game.appid in bronze_game_ids,
                "bronze_reviews": bronze_review_counts.get(game.appid, 0),
                "silver_game": game.appid in silver_game_ids,
                "silver_reviews": silver_review_counts.get(game.appid, 0),
                "gold_reviews": gold_review_counts.get(game.appid, 0),
                "mongo_game_metrics": game.appid in mongo_ids,
            }
            state["passed"] = (
                state["bronze_metadata"]
                and state["bronze_reviews"] == expected
                and state["silver_game"]
                and state["silver_reviews"] == expected
                and state["gold_reviews"] == expected
                and state["mongo_game_metrics"]
            )
            games[str(game.appid)] = state

        payload = {
            "batch_id": manifest.batch_id,
            "passed": all(row["passed"] for row in games.values()),
            "games": games,
        }
        args.out.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.out.parent / f"{args.out.name}.tmp"
        temporary.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        temporary.replace(args.out)
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0 if payload["passed"] else 1
    finally:
        if client is not None:
            client.close()
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
