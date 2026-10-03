"""Serving layer: reads the local Steam JSONL snapshot and computes the
analytics payloads consumed by the frontend dashboard.

Data sources (historical snapshot, local dev stand-in for MongoDB serving):
- data/raw/steam/selected_50_games.jsonl          -> game metadata (name, genres, is_free)
- data/raw/steam/landing/reviews_by_game/*.jsonl  -> sampled reviews per game

Playtime buckets follow docs/07_SPARK_ANALYTICS.md (Q2):
0-2h, 2-10h, 10-50h, 50+h.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
GAMES_FILE = REPO_ROOT / "data" / "raw" / "steam" / "selected_50_games.jsonl"
REVIEWS_DIR = REPO_ROOT / "data" / "raw" / "steam" / "landing" / "reviews_by_game"

# (label, lower_minutes_inclusive, upper_minutes_exclusive)
PLAYTIME_BUCKETS: list[tuple[str, float, float]] = [
    ("0-2h", 0, 120),
    ("2-10h", 120, 600),
    ("10-50h", 600, 3000),
    ("50+h", 3000, float("inf")),
]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _bucket_for(minutes: float | None) -> str:
    if minutes is None:
        return "unknown"
    for label, lower, upper in PLAYTIME_BUCKETS:
        if lower <= minutes < upper:
            return label
    return "unknown"


class AnalyticsStore:
    """Loads the snapshot once, then serves pre-computed analytics."""

    def __init__(self) -> None:
        self.games_meta: dict[int, dict[str, Any]] = {}
        self.reviews: list[dict[str, Any]] = []
        self._loaded = False

    def load(self) -> None:
        if self._loaded:
            return
        if GAMES_FILE.exists():
            for rec in _read_jsonl(GAMES_FILE):
                self.games_meta[int(rec["appid"])] = rec

        if REVIEWS_DIR.exists():
            for path in sorted(REVIEWS_DIR.glob("*.jsonl")):
                for rec in _read_jsonl(path):
                    review = rec.get("review") or {}
                    author = review.get("author") or {}
                    self.reviews.append(
                        {
                            "recommendationid": review.get("recommendationid"),
                            "appid": int(rec.get("appid") or path.stem),
                            "game_name": rec.get("game_name"),
                            "voted_up": bool(review.get("voted_up")),
                            "playtime_at_review": author.get("playtime_at_review"),
                            "timestamp_created": (review.get("timestamp_created") or 0) * 1000,
                        }
                    )
        self.reviews.sort(key=lambda r: r["timestamp_created"], reverse=True)
        self._loaded = True

    # ---- analytics -----------------------------------------------------

    def games_analytics(self) -> list[dict[str, Any]]:
        per_game: dict[int, dict[str, Any]] = {}
        for appid, meta in self.games_meta.items():
            per_game[appid] = {
                "appid": appid,
                "game_name": meta.get("name") or f"App {appid}",
                "genres": meta.get("genres") or [],
                "is_free": bool(meta.get("is_free")),
                "review_count": 0,
                "positive_reviews": 0,
                "negative_reviews": 0,
            }
        for review in self.reviews:
            stats = per_game.setdefault(
                review["appid"],
                {
                    "appid": review["appid"],
                    "game_name": review.get("game_name") or f"App {review['appid']}",
                    "genres": [],
                    "is_free": False,
                    "review_count": 0,
                    "positive_reviews": 0,
                    "negative_reviews": 0,
                },
            )
            stats["review_count"] += 1
            if review["voted_up"]:
                stats["positive_reviews"] += 1
            else:
                stats["negative_reviews"] += 1
        for stats in per_game.values():
            total = stats["review_count"]
            stats["recommendation_rate"] = (
                stats["positive_reviews"] / total if total else 0.0
            )
        return list(per_game.values())

    def genre_analytics(self) -> list[dict[str, Any]]:
        games = {g["appid"]: g for g in self.games_analytics()}
        per_genre: dict[str, dict[str, Any]] = {}
        for appid, game in games.items():
            for genre in game["genres"] or ["Unknown"]:
                stats = per_genre.setdefault(
                    genre,
                    {
                        "genre": genre,
                        "game_count": 0,
                        "review_count": 0,
                        "positive_reviews": 0,
                    },
                )
                stats["game_count"] += 1
                stats["review_count"] += game["review_count"]
                stats["positive_reviews"] += game["positive_reviews"]
        for stats in per_genre.values():
            total = stats["review_count"]
            stats["recommendation_rate"] = (
                stats["positive_reviews"] / total if total else 0.0
            )
        return list(per_genre.values())

    def playtime_analytics(self) -> list[dict[str, Any]]:
        per_bucket: dict[str, dict[str, Any]] = {}
        for review in self.reviews:
            bucket = _bucket_for(review["playtime_at_review"])
            stats = per_bucket.setdefault(
                bucket,
                {"playtime_bucket": bucket, "review_count": 0, "positive_reviews": 0},
            )
            stats["review_count"] += 1
            if review["voted_up"]:
                stats["positive_reviews"] += 1
        for stats in per_bucket.values():
            total = stats["review_count"]
            stats["recommendation_rate"] = (
                stats["positive_reviews"] / total if total else 0.0
            )
        ordered = [b[0] for b in PLAYTIME_BUCKETS] + ["unknown"]
        return sorted(
            per_bucket.values(),
            key=lambda s: ordered.index(s["playtime_bucket"])
            if s["playtime_bucket"] in ordered
            else len(ordered),
        )

    def free_paid_analytics(self) -> list[dict[str, Any]]:
        games = self.games_analytics()
        groups: dict[bool, dict[str, Any]] = {}
        for game in games:
            stats = groups.setdefault(
                game["is_free"],
                {
                    "is_free": game["is_free"],
                    "segment": "Free" if game["is_free"] else "Paid",
                    "game_count": 0,
                    "review_count": 0,
                    "positive_reviews": 0,
                },
            )
            stats["game_count"] += 1
            stats["review_count"] += game["review_count"]
            stats["positive_reviews"] += game["positive_reviews"]
        for stats in groups.values():
            total = stats["review_count"]
            stats["recommendation_rate"] = (
                stats["positive_reviews"] / total if total else 0.0
            )
        return list(groups.values())

    def recent_reviews(self, page: int, page_size: int) -> tuple[list[dict[str, Any]], int]:
        total = len(self.reviews)
        start = max(page - 1, 0) * page_size
        return self.reviews[start : start + page_size], total


_store = AnalyticsStore()
_lock = threading.Lock()


def get_store() -> AnalyticsStore:
    if not _store._loaded:
        with _lock:
            _store.load()
    return _store
