from app.database.mongodb import get_collection
from app.repositories.mongo_utils import serialize_document


def _realtime_pipeline(collection_name: str) -> list[dict] | None:
    vote_counts = {
        "positive_reviews": {
            "$sum": {"$cond": [{"$eq": ["$voted_up", True]}, 1, 0]}
        },
        "negative_reviews": {
            "$sum": {"$cond": [{"$eq": ["$voted_up", False]}, 1, 0]}
        },
        "review_count": {"$sum": 1},
    }
    if collection_name == "game_metrics":
        return [
            {
                "$group": {
                    "_id": "$appid",
                    "game_name": {"$max": "$game_name"},
                    **vote_counts,
                }
            },
            {
                "$set": {
                    "recommendation_rate": {
                        "$divide": ["$positive_reviews", "$review_count"]
                    }
                }
            },
        ]
    if collection_name == "genre_metrics":
        return [
            {
                "$set": {
                    "genres": {
                        "$ifNull": [
                            {"$cond": [{"$isArray": "$genres"}, "$genres", []]},
                            ["UNKNOWN"],
                        ]
                    }
                }
            },
            {
                "$set": {
                    "genres": {
                        "$cond": [{"$eq": ["$genres", []]}, ["UNKNOWN"], "$genres"]
                    }
                }
            },
            {"$unwind": "$genres"},
            {
                "$group": {
                    "_id": "$genres",
                    **vote_counts,
                }
            },
            {
                "$set": {
                    "recommendation_rate": {
                        "$divide": ["$positive_reviews", "$review_count"]
                    }
                }
            },
        ]
    if collection_name == "playtime_metrics":
        return [
            {
                "$set": {
                    "playtime_hours": {
                        "$ifNull": [
                            "$playtime_hours",
                            {"$divide": ["$playtime_at_review", 60.0]},
                        ]
                    }
                }
            },
            {
                "$set": {
                    "playtime_bucket": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {
                                        "$or": [
                                            {"$eq": ["$playtime_hours", None]},
                                            {"$lt": ["$playtime_hours", 0]},
                                        ]
                                    },
                                    "then": None,
                                },
                                {
                                    "case": {"$lt": ["$playtime_hours", 2]},
                                    "then": "0-2h",
                                },
                                {
                                    "case": {"$lt": ["$playtime_hours", 10]},
                                    "then": "2-10h",
                                },
                                {
                                    "case": {"$lt": ["$playtime_hours", 50]},
                                    "then": "10-50h",
                                },
                            ],
                            "default": "50h+",
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$playtime_bucket",
                    **vote_counts,
                    "playtime_observed_count": {
                        "$sum": {
                            "$cond": [{"$ne": ["$playtime_hours", None]}, 1, 0]
                        }
                    },
                    "avg_playtime_hours": {"$avg": "$playtime_hours"},
                }
            },
            {
                "$set": {
                    "recommendation_rate": {
                        "$divide": ["$positive_reviews", "$review_count"]
                    }
                }
            },
        ]
    if collection_name == "free_paid_metrics":
        return [
            {
                "$set": {
                    "game_type": {"$ifNull": ["$game_type", "UNKNOWN"]}
                }
            },
            {
                "$group": {
                    "_id": "$game_type",
                    **vote_counts,
                    "stream_game_ids": {"$addToSet": "$appid"},
                    "playtime_observed_count": {
                        "$sum": {
                            "$cond": [
                                {"$ne": ["$playtime_hours", None]},
                                1,
                                0,
                            ]
                        }
                    },
                    "avg_playtime_hours": {"$avg": "$playtime_hours"},
                }
            },
            {
                "$set": {
                    "recommendation_rate": {
                        "$divide": ["$positive_reviews", "$review_count"]
                    },
                    "stream_game_count": {"$size": "$stream_game_ids"},
                }
            },
        ]
    return None


def _merge_count_metrics(
    batch: list[dict],
    streamed: list[dict],
    key_field: str,
) -> list[dict]:
    merged = {item.get(key_field): dict(item) for item in batch}
    for item in merged.values():
        if "review_count" not in item:
            continue
        item["batch_review_count"] = int(item["review_count"])
        item["stream_review_count"] = 0
        if "avg_playtime_hours" in item:
            item["batch_playtime_observed_count"] = int(
                item.get("playtime_observed_count", 0)
            )
            item["stream_playtime_observed_count"] = 0

    for contribution in streamed:
        key = contribution.get("_id")
        current = merged.setdefault(key, {key_field: key})
        batch_count = int(current.get("batch_review_count", current.get("review_count", 0)))
        stream_count = int(contribution.get("review_count", 0))
        current["batch_review_count"] = batch_count
        current["stream_review_count"] = (
            int(current.get("stream_review_count", 0)) + stream_count
        )
        current["review_count"] = batch_count + current["stream_review_count"]
        current["positive_reviews"] = int(current.get("positive_reviews", 0)) + int(
            contribution.get("positive_reviews", 0)
        )
        current["negative_reviews"] = int(current.get("negative_reviews", 0)) + int(
            contribution.get("negative_reviews", 0)
        )
        current["recommendation_rate"] = (
            current["positive_reviews"] / current["review_count"]
            if current["review_count"]
            else 0.0
        )
        if "avg_playtime_hours" in contribution:
            batch_observed = int(current.get("batch_playtime_observed_count", 0))
            stream_observed = int(contribution.get("playtime_observed_count", 0))
            current["stream_playtime_observed_count"] = stream_observed
            current["playtime_observed_count"] = batch_observed + stream_observed
            if current["playtime_observed_count"]:
                weighted_hours = (
                    float(current.get("avg_playtime_hours") or 0.0) * batch_observed
                    + float(contribution.get("avg_playtime_hours") or 0.0)
                    * stream_observed
                )
                current["avg_playtime_hours"] = (
                    weighted_hours / current["playtime_observed_count"]
                )
        if "stream_game_count" in contribution:
            current["stream_game_count"] = int(contribution["stream_game_count"])
        if "game_type" not in current and key_field == "game_type":
            current["game_type"] = key
        if "genre" not in current and key_field == "genre":
            current["genre"] = key
        if "playtime_bucket" not in current and key_field == "playtime_bucket":
            current["playtime_bucket"] = key
        if "appid" not in current and key_field == "appid":
            current["appid"] = key
            current["game_name"] = contribution.get("game_name") or f"Unknown game ({key})"
        current["serving_snapshot"] = "batch_plus_streaming"

    for item in merged.values():
        count = int(item.get("review_count", 0))
        if count:
            item["recommendation_rate"] = (
                int(item.get("positive_reviews", 0)) / count
            )
    return list(merged.values())


class AnalyticsRepository:
    async def find_all(self, collection_name: str) -> list[dict]:
        collection = get_collection(collection_name)
        batch = [
            serialize_document(document)
            async for document in collection.find({})
        ]
        pipeline = _realtime_pipeline(collection_name)
        if pipeline is None:
            return batch

        realtime_collection = get_collection("recent_reviews")
        streamed = [
            dict(document)
            async for document in realtime_collection.aggregate(pipeline)
        ]
        key_fields = {
            "game_metrics": "appid",
            "genre_metrics": "genre",
            "playtime_metrics": "playtime_bucket",
            "free_paid_metrics": "game_type",
        }
        key_field = key_fields[collection_name]
        if collection_name == "game_metrics":
            batch_names = {
                item.get("appid"): item.get("game_name")
                for item in batch
            }
            for item in streamed:
                item["game_name"] = item.get("game_name") or batch_names.get(
                    item.get("_id")
                ) or f"Unknown game ({item.get('_id')})"
        return _merge_count_metrics(batch, streamed, key_field)

    async def find_top_games(self, limit: int) -> list[dict]:
        games = await self.find_all("game_metrics")
        games.sort(
            key=lambda item: (
                -float(item.get("recommendation_rate", 0.0)),
                -int(item.get("review_count", 0)),
                int(item.get("appid", 0)),
            )
        )
        return games[:limit]