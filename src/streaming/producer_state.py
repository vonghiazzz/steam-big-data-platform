"""Durable SQLite state for the bounded Steam review producer."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Mapping


class ProducerState:
    """Persist bootstrap, business-key deduplication, and an event outbox."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self._create_schema()

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS game_state (
                appid INTEGER PRIMARY KEY,
                initialized INTEGER NOT NULL DEFAULT 0,
                high_water_timestamp INTEGER,
                last_polled_at TEXT
            );

            CREATE TABLE IF NOT EXISTS seen_review (
                appid INTEGER NOT NULL,
                recommendationid TEXT NOT NULL,
                timestamp_created INTEGER,
                source TEXT NOT NULL,
                PRIMARY KEY (appid, recommendationid)
            );

            CREATE TABLE IF NOT EXISTS event_outbox (
                event_id TEXT PRIMARY KEY,
                appid INTEGER NOT NULL,
                recommendationid TEXT NOT NULL,
                event_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                published_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_outbox_pending
                ON event_outbox (appid, published_at);
            """
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "ProducerState":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def is_initialized(self, appid: int) -> bool:
        row = self.connection.execute(
            "SELECT initialized FROM game_state WHERE appid = ?",
            (appid,),
        ).fetchone()
        return bool(row and row["initialized"])

    def has_seen(self, appid: int, recommendationid: str) -> bool:
        row = self.connection.execute(
            """
            SELECT 1 FROM seen_review
            WHERE appid = ? AND recommendationid = ?
            """,
            (appid, recommendationid),
        ).fetchone()
        return row is not None

    def seed_seen_reviews(
        self,
        appid: int,
        reviews: Iterable[Mapping[str, Any]],
        *,
        source: str,
    ) -> int:
        rows = []
        for review in reviews:
            recommendationid = str(
                review.get("recommendationid") or ""
            ).strip()
            if not recommendationid:
                continue
            rows.append(
                (
                    appid,
                    recommendationid,
                    _optional_int(review.get("timestamp_created")),
                    source,
                )
            )
        before = self.connection.total_changes
        self.connection.executemany(
            """
            INSERT OR IGNORE INTO seen_review
                (appid, recommendationid, timestamp_created, source)
            VALUES (?, ?, ?, ?)
            """,
            rows,
        )
        self.connection.commit()
        return self.connection.total_changes - before

    def complete_bootstrap(
        self,
        appid: int,
        reviews: Iterable[Mapping[str, Any]],
        *,
        polled_at: str,
    ) -> None:
        reviews = list(reviews)
        self.seed_seen_reviews(appid, reviews, source="BOOTSTRAP")
        high_water = max(
            (
                _optional_int(review.get("timestamp_created")) or 0
                for review in reviews
            ),
            default=0,
        )
        self.connection.execute(
            """
            INSERT INTO game_state
                (appid, initialized, high_water_timestamp, last_polled_at)
            VALUES (?, 1, ?, ?)
            ON CONFLICT(appid) DO UPDATE SET
                initialized = 1,
                high_water_timestamp = MAX(
                    COALESCE(game_state.high_water_timestamp, 0),
                    excluded.high_water_timestamp
                ),
                last_polled_at = excluded.last_polled_at
            """,
            (appid, high_water, polled_at),
        )
        self.connection.commit()

    def record_poll(self, appid: int, *, polled_at: str) -> None:
        self.connection.execute(
            """
            UPDATE game_state SET last_polled_at = ? WHERE appid = ?
            """,
            (polled_at, appid),
        )
        self.connection.commit()

    def enqueue_events(self, events: Iterable[Mapping[str, Any]]) -> int:
        rows = []
        for event in events:
            payload = event["payload"]
            rows.append(
                (
                    event["event_id"],
                    int(event["appid"]),
                    str(payload["recommendationid"]),
                    json.dumps(event, ensure_ascii=False, separators=(",", ":")),
                    str(event["produced_at"]),
                )
            )
        before = self.connection.total_changes
        self.connection.executemany(
            """
            INSERT OR IGNORE INTO event_outbox
                (event_id, appid, recommendationid, event_json, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.connection.commit()
        return self.connection.total_changes - before

    def pending_events(self, appid: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT event_json FROM event_outbox
            WHERE appid = ? AND published_at IS NULL
            ORDER BY created_at, event_id
            """,
            (appid,),
        ).fetchall()
        return [json.loads(row["event_json"]) for row in rows]

    def mark_published(self, event: Mapping[str, Any], *, published_at: str) -> None:
        payload = event["payload"]
        with self.connection:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO seen_review
                    (appid, recommendationid, timestamp_created, source)
                VALUES (?, ?, ?, 'PUBLISHED')
                """,
                (
                    int(event["appid"]),
                    str(payload["recommendationid"]),
                    _optional_int(payload.get("timestamp_created")),
                ),
            )
            self.connection.execute(
                """
                UPDATE event_outbox SET published_at = ? WHERE event_id = ?
                """,
                (published_at, event["event_id"]),
            )


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
