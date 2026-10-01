"""Durable SQLite outbox and polling state for player-count snapshots."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Mapping


class PlayerCountState:
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
                last_player_count INTEGER,
                last_observed_at TEXT,
                last_polled_at TEXT
            );

            CREATE TABLE IF NOT EXISTS event_outbox (
                event_id TEXT PRIMARY KEY,
                appid INTEGER NOT NULL,
                event_json TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                published_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_player_count_outbox_pending
                ON event_outbox (appid, published_at, observed_at);
            """
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "PlayerCountState":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def enqueue_event(self, event: Mapping[str, Any]) -> bool:
        before = self.connection.total_changes
        self.connection.execute(
            """
            INSERT OR IGNORE INTO event_outbox
                (event_id, appid, event_json, observed_at, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                str(event["event_id"]),
                int(event["appid"]),
                json.dumps(event, ensure_ascii=False, separators=(",", ":")),
                str(event["event_time"]),
                str(event["produced_at"]),
            ),
        )
        self.connection.commit()
        return self.connection.total_changes > before

    def pending_events(self, appid: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT event_json FROM event_outbox
            WHERE appid = ? AND published_at IS NULL
            ORDER BY observed_at, event_id
            """,
            (appid,),
        ).fetchall()
        return [json.loads(row["event_json"]) for row in rows]

    def mark_published(self, event_id: str, *, published_at: str) -> None:
        self.connection.execute(
            """
            UPDATE event_outbox
            SET published_at = ?
            WHERE event_id = ? AND published_at IS NULL
            """,
            (published_at, event_id),
        )
        self.connection.commit()

    def record_poll(
        self,
        appid: int,
        *,
        player_count: int,
        observed_at: str,
        polled_at: str,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO game_state
                (appid, last_player_count, last_observed_at, last_polled_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(appid) DO UPDATE SET
                last_player_count = excluded.last_player_count,
                last_observed_at = excluded.last_observed_at,
                last_polled_at = excluded.last_polled_at
            """,
            (appid, player_count, observed_at, polled_at),
        )
        self.connection.commit()
