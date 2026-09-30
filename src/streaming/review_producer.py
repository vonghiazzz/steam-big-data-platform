"""Bounded ACTIVE-game Steam poller for REVIEW_CREATED events only."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

import requests
from dotenv import load_dotenv

from src.discovery.registry import (
    STATUS_ACTIVE,
    STATUS_RETIRED,
    RegistryEntry,
    load_registry,
)
from src.streaming.event_contract import build_review_created_event
from src.streaming.producer_state import ProducerState


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env", override=False)
DEFAULT_REGISTRY_PATH = PROJECT_ROOT / "data/raw/registry/game_registry.jsonl"
DEFAULT_STATE_PATH = (
    PROJECT_ROOT / "data/state/streaming/review_producer_v1.sqlite3"
)
DEFAULT_CANONICAL_REVIEWS = PROJECT_ROOT / "data/raw/bronze_ready/reviews_by_game"
DEFAULT_HDFS_REVIEWS_ROOT = "/steam/bronze/reviews"


class ReviewClient(Protocol):
    def fetch_page(self, appid: int, cursor: str) -> Mapping[str, Any]: ...


class EventPublisher(Protocol):
    def send(self, appid: int, event: Mapping[str, Any]) -> None: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...


class BaselineReviewSource(Protocol):
    def load_reviews(self, appid: int) -> list[Mapping[str, Any]]: ...


@dataclass(frozen=True)
class PollResult:
    appid: int
    bootstrap: bool
    observed: int
    emitted: int
    hit_known_review: bool


class SafetyGapError(RuntimeError):
    """Raised when bounded polling cannot reconnect to known state."""


class SteamReviewClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.url_template = "https://store.steampowered.com/appreviews/{appid}"
        self.headers = {"User-Agent": "Mozilla/5.0 BDA501-Educational-Project"}

    def fetch_page(self, appid: int, cursor: str) -> Mapping[str, Any]:
        params = {
            "json": 1,
            "filter": "recent",
            "language": "english",
            "review_type": "all",
            "purchase_type": "all",
            "num_per_page": 100,
            "cursor": cursor,
            "filter_offtopic_activity": 1,
        }
        max_attempts = int(os.getenv("STREAM_API_MAX_ATTEMPTS", "3"))
        for attempt in range(1, max_attempts + 1):
            try:
                response = self.session.get(
                    self.url_template.format(appid=appid),
                    params=params,
                    headers=self.headers,
                    timeout=30,
                )
                if response.status_code == 429 or response.status_code >= 500:
                    if attempt == max_attempts:
                        response.raise_for_status()
                    retry_after = response.headers.get("Retry-After")
                    delay = (
                        int(retry_after)
                        if retry_after and retry_after.isdigit()
                        else min(2**attempt, 30)
                    )
                    time.sleep(delay)
                    continue
                response.raise_for_status()
                payload = response.json()
                if payload.get("success") != 1:
                    raise RuntimeError(
                        f"Steam success={payload.get('success')} for appid={appid}"
                    )
                return payload
            except requests.RequestException:
                if attempt == max_attempts:
                    raise
                time.sleep(min(2**attempt, 30))
        raise RuntimeError(f"Steam request attempts exhausted for appid={appid}")


class HdfsBronzeReviewSource:
    """Read the canonical per-game Bronze JSONL through the NameNode CLI."""

    def __init__(
        self,
        *,
        root: str,
        namenode_container: str,
        max_ids_per_game: int,
    ) -> None:
        self.root = root.rstrip("/")
        self.namenode_container = namenode_container
        self.max_ids_per_game = max_ids_per_game

    def load_reviews(self, appid: int) -> list[Mapping[str, Any]]:
        hdfs_path = f"{self.root}/{appid}.jsonl"
        try:
            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    self.namenode_container,
                    "hdfs",
                    "dfs",
                    "-cat",
                    hdfs_path,
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            detail = getattr(exc, "stderr", "") or str(exc)
            raise RuntimeError(
                f"cannot read canonical HDFS Bronze {hdfs_path}: {detail.strip()}"
            ) from exc
        reviews = _parse_bronze_review_lines(result.stdout.splitlines())
        reviews.sort(
            key=lambda item: int(item.get("timestamp_created") or 0),
            reverse=True,
        )
        return reviews[: self.max_ids_per_game]


class LocalJsonlReviewSource:
    """Test-only/local-fixture baseline source; never the production default."""

    def __init__(self, root: Path, *, max_ids_per_game: int) -> None:
        self.root = root
        self.max_ids_per_game = max_ids_per_game

    def load_reviews(self, appid: int) -> list[Mapping[str, Any]]:
        path = self.root / f"{appid}.jsonl"
        if not path.exists():
            return []
        with path.open("r", encoding="utf-8") as handle:
            reviews = _parse_bronze_review_lines(handle)
        reviews.sort(
            key=lambda item: int(item.get("timestamp_created") or 0),
            reverse=True,
        )
        return reviews[: self.max_ids_per_game]


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


class ReviewProducerService:
    def __init__(
        self,
        *,
        state: ProducerState,
        client: ReviewClient,
        publisher: EventPublisher,
        max_pages: int,
        request_delay_seconds: float,
        baseline_source: BaselineReviewSource,
        clock: Callable[[], str] | None = None,
    ) -> None:
        if max_pages <= 0:
            raise ValueError("max_pages must be positive")
        self.state = state
        self.client = client
        self.publisher = publisher
        self.max_pages = max_pages
        self.request_delay_seconds = request_delay_seconds
        self.baseline_source = baseline_source
        self.clock = clock or utc_now

    def poll_game(self, appid: int) -> PollResult:
        initialized = self.state.is_initialized(appid)
        if not initialized:
            self._seed_canonical_history(appid)

        observed: list[Mapping[str, Any]] = []
        hit_known = False
        cursor = "*"

        for page_number in range(1, self.max_pages + 1):
            payload = self.client.fetch_page(appid, cursor)
            page_reviews = payload.get("reviews") or []
            if not isinstance(page_reviews, list):
                raise RuntimeError(f"Steam reviews is not a list for appid={appid}")

            for review in page_reviews:
                if not isinstance(review, Mapping):
                    continue
                recommendationid = str(
                    review.get("recommendationid") or ""
                ).strip()
                if not recommendationid:
                    continue
                if self.state.has_seen(appid, recommendationid):
                    hit_known = True
                else:
                    observed.append(review)

            next_cursor = str(payload.get("cursor") or "").strip()
            if hit_known or not page_reviews or not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor
            if page_number < self.max_pages and self.request_delay_seconds > 0:
                time.sleep(self.request_delay_seconds)

        polled_at = self.clock()
        if not initialized:
            # First contact establishes "now" without emitting historical rows.
            self.state.complete_bootstrap(appid, observed, polled_at=polled_at)
            return PollResult(appid, True, len(observed), 0, hit_known)

        if observed and not hit_known:
            raise SafetyGapError(
                f"appid={appid}: no overlap with known recommendationids within "
                f"{self.max_pages} bounded page(s); refusing to publish"
            )

        events = [
            build_review_created_event(
                appid,
                review,
                produced_at=polled_at,
            )
            for review in sorted(
                observed,
                key=lambda item: (
                    int(item.get("timestamp_created") or 0),
                    str(item.get("recommendationid") or ""),
                ),
            )
        ]
        self.state.enqueue_events(events)

        emitted = 0
        for event in self.state.pending_events(appid):
            self.publisher.send(appid, event)
            self.state.mark_published(event, published_at=self.clock())
            emitted += 1
        self.publisher.flush()
        self.state.record_poll(appid, polled_at=polled_at)
        return PollResult(appid, False, len(observed), emitted, hit_known)

    def _seed_canonical_history(self, appid: int) -> None:
        reviews = self.baseline_source.load_reviews(appid)
        self.state.seed_seen_reviews(
            appid,
            reviews,
            source="CANONICAL_BATCH_BASELINE",
        )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_bronze_review_lines(lines) -> list[Mapping[str, Any]]:
    reviews: list[Mapping[str, Any]] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        review = row.get("review")
        if isinstance(review, Mapping):
            reviews.append(review)
    return reviews


def load_active_scope(
    registry_path: Path,
) -> tuple[list[RegistryEntry], dict[str, int]]:
    registry = load_registry(registry_path)
    entries = list(registry.values())
    counts = {
        "total": len(entries),
        "active": sum(entry.status == STATUS_ACTIVE for entry in entries),
        "retired": sum(entry.status == STATUS_RETIRED for entry in entries),
    }
    active = sorted(
        (entry for entry in entries if entry.status == STATUS_ACTIVE),
        key=lambda entry: entry.appid,
    )
    return active, counts


def ensure_disk_safety(path: Path, minimum_free_gb: float) -> None:
    free_bytes = shutil.disk_usage(path).free
    minimum_bytes = int(minimum_free_gb * 1024**3)
    if free_bytes < minimum_bytes:
        raise RuntimeError(
            f"disk safety stop: {free_bytes / 1024**3:.2f} GiB free, "
            f"minimum is {minimum_free_gb:.2f} GiB"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="run one bounded poll")
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY_PATH)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE_PATH)
    parser.add_argument(
        "--canonical-reviews",
        type=Path,
        default=DEFAULT_CANONICAL_REVIEWS,
    )
    parser.add_argument(
        "--bootstrap-source",
        choices=("hdfs", "local"),
        default=os.getenv("STREAM_BOOTSTRAP_SOURCE", "hdfs"),
        help="production default is canonical HDFS; local is test-only",
    )
    parser.add_argument(
        "--hdfs-reviews-root",
        default=os.getenv("HDFS_BRONZE_REVIEWS", DEFAULT_HDFS_REVIEWS_ROOT),
    )
    parser.add_argument(
        "--max-games",
        type=int,
        help="bounded smoke-test subset selected only from ACTIVE entries",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not args.registry.exists():
        print(f"Registry does not exist: {args.registry}")
        return 2

    active, counts = load_active_scope(args.registry)
    print(f"registry_total={counts['total']}")
    print(f"active_game_count={counts['active']}")
    print(f"retired_game_count={counts['retired']}")

    max_active = int(os.getenv("STREAM_MAX_ACTIVE_GAMES", "100"))
    if counts["active"] > max_active:
        print(
            f"Safety stop: ACTIVE count {counts['active']} exceeds "
            f"STREAM_MAX_ACTIVE_GAMES={max_active}"
        )
        return 2
    if not active:
        print("No ACTIVE games; producer stopped cleanly")
        return 0
    if args.max_games is not None:
        if args.max_games <= 0:
            print("--max-games must be positive")
            return 2
        active = active[: args.max_games]
    print(f"poll_scope_count={len(active)}")

    poll_interval = int(os.getenv("STREAM_POLL_INTERVAL_SECONDS", "600"))
    minimum_poll_interval = int(
        os.getenv("STREAM_MIN_POLL_INTERVAL_SECONDS", "300")
    )
    if not args.once and poll_interval < minimum_poll_interval:
        print(
            f"Safety stop: STREAM_POLL_INTERVAL_SECONDS={poll_interval} is below "
            f"STREAM_MIN_POLL_INTERVAL_SECONDS={minimum_poll_interval}"
        )
        return 2
    max_pages = int(os.getenv("STREAM_MAX_PAGES_PER_GAME", "3"))
    request_delay = float(os.getenv("STREAM_REQUEST_DELAY_SECONDS", "1.0"))
    minimum_free_gb = float(os.getenv("STREAM_MIN_FREE_GB", "5"))
    max_baseline_ids = int(
        os.getenv("STREAM_BOOTSTRAP_MAX_IDS_PER_GAME", "1000")
    )
    if max_baseline_ids <= 0:
        print("STREAM_BOOTSTRAP_MAX_IDS_PER_GAME must be positive")
        return 2

    if args.bootstrap_source == "hdfs":
        baseline_source: BaselineReviewSource = HdfsBronzeReviewSource(
            root=args.hdfs_reviews_root,
            namenode_container=os.getenv(
                "HADOOP_NAMENODE_CONTAINER",
                "bda501-namenode",
            ),
            max_ids_per_game=max_baseline_ids,
        )
    else:
        baseline_source = LocalJsonlReviewSource(
            args.canonical_reviews,
            max_ids_per_game=max_baseline_ids,
        )
    print(f"bootstrap_source={args.bootstrap_source}")

    publisher = KafkaEventPublisher(
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        ),
        topic=os.getenv("KAFKA_TOPIC", "steam_events"),
        compression_type=os.getenv("KAFKA_COMPRESSION_TYPE", "gzip"),
    )
    client = SteamReviewClient()

    try:
        with ProducerState(args.state) as state:
            service = ReviewProducerService(
                state=state,
                client=client,
                publisher=publisher,
                max_pages=max_pages,
                request_delay_seconds=request_delay,
                baseline_source=baseline_source,
            )
            while True:
                try:
                    ensure_disk_safety(PROJECT_ROOT, minimum_free_gb)
                except RuntimeError as exc:
                    print(exc)
                    return 2
                cycle_emitted = 0
                for index, entry in enumerate(active):
                    try:
                        result = service.poll_game(entry.appid)
                        cycle_emitted += result.emitted
                        print(
                            f"appid={entry.appid} bootstrap={result.bootstrap} "
                            f"observed={result.observed} emitted={result.emitted}"
                        )
                    except Exception as exc:
                        print(f"appid={entry.appid} poll_error={exc}")
                    if index + 1 < len(active) and request_delay > 0:
                        time.sleep(request_delay)
                print(f"cycle_emitted={cycle_emitted}")
                if args.once:
                    break
                time.sleep(poll_interval)
    finally:
        publisher.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
