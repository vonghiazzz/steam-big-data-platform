"""Shared idempotent Kafka JSON event publisher."""

from __future__ import annotations

import json
from typing import Any, Mapping


class KafkaEventPublisher:
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        compression_type: str,
    ) -> None:
        try:
            from kafka import KafkaProducer
        except ImportError as exc:
            raise RuntimeError(
                "kafka-python is required; install requirements.txt"
            ) from exc

        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=[
                item.strip()
                for item in bootstrap_servers.split(",")
                if item.strip()
            ],
            key_serializer=lambda value: str(value).encode("utf-8"),
            value_serializer=lambda value: json.dumps(
                value,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8"),
            compression_type=compression_type,
            acks="all",
            enable_idempotence=True,
            linger_ms=100,
        )

    def send(self, appid: int, event: Mapping[str, Any]) -> None:
        self.producer.send(self.topic, key=appid, value=dict(event)).get(
            timeout=30
        )

    def flush(self) -> None:
        self.producer.flush(timeout=30)

    def close(self) -> None:
        self.producer.close(timeout=30)
