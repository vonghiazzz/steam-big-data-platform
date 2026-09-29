"""Publish one explicitly test-only Kafka fixture for bounded live validation."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone

from kafka import KafkaProducer

from src.streaming.event_contract import build_review_created_event


TEST_APPID = 570
TEST_RECOMMENDATION_ID = "TEST_ONLY_STREAMING_V1_20260929_000001"


def valid_event() -> dict:
    now = datetime.now(timezone.utc)
    timestamp = int(now.timestamp())
    return build_review_created_event(
        TEST_APPID,
        {
            "recommendationid": TEST_RECOMMENDATION_ID,
            "author": {
                "steamid": "TEST_ONLY_USER",
                "num_games_owned": 1,
                "num_reviews": 1,
                "playtime_forever": 180,
                "playtime_at_review": 120,
                "last_played": timestamp,
            },
            "language": "english",
            "review": "TEST ONLY - bounded Streaming V1 smoke fixture",
            "timestamp_created": timestamp,
            "timestamp_updated": timestamp,
            "voted_up": True,
            "votes_up": 0,
            "votes_funny": 0,
            "weighted_vote_score": "0",
            "comment_count": 0,
            "steam_purchase": False,
            "received_for_free": True,
            "refunded": False,
            "written_during_early_access": False,
        },
        produced_at=now.isoformat(),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("valid", "malformed"))
    args = parser.parse_args()
    payload = (
        valid_event()
        if args.kind == "valid"
        else {
            "test_only": True,
            "fixture": "MALFORMED_STREAMING_V1_20260929_000001",
        }
    )

    producer = KafkaProducer(
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        ),
        key_serializer=lambda value: str(value).encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value,
            separators=(",", ":"),
        ).encode("utf-8"),
        compression_type="gzip",
        acks="all",
        enable_idempotence=True,
    )
    try:
        metadata = producer.send(
            os.getenv("KAFKA_TOPIC", "steam_events_test"),
            key=TEST_APPID,
            value=payload,
        ).get(timeout=30)
        producer.flush(timeout=30)
    finally:
        producer.close(timeout=30)
    print(
        f"fixture={args.kind} topic={metadata.topic} "
        f"partition={metadata.partition} offset={metadata.offset}"
    )


if __name__ == "__main__":
    main()
