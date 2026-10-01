"""Bounded ACTIVE-game producer for PLAYER_COUNT_SNAPSHOT events."""

from __future__ import annotations

import argparse
import os
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

import requests
from dotenv import load_dotenv

from src.discovery.registry import STATUS_ACTIVE, STATUS_RETIRED, load_registry
from src.streaming.event_contract import build_player_count_snapshot_event
from src.streaming.kafka_publisher import KafkaEventPublisher
from src.streaming.player_count_state import PlayerCountState


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env", override=False)
DEFAULT_REGISTRY_PATH = PROJECT_ROOT / "data/raw/registry/game_registry.jsonl"
DEFAULT_STATE_PATH = (
    PROJECT_ROOT / "data/state/streaming/player_count_producer_v1.sqlite3"
)
DEFAULT_ENDPOINT = (
    "https://api.steampowered.com/"
    "ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
)


class PlayerCountClient(Protocol):
    def fetch_count(self, appid: int) -> int: ...


class EventPublisher(Protocol):
    def send(self, appid: int, event: Mapping[str, Any]) -> None: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class PlayerCountPollResult:
    appid: int
    player_count: int
    observed_at: str
    emitted: int


class SteamPlayerCountClient:
    def __init__(self, endpoint: str = DEFAULT_ENDPOINT) -> None:
        self.endpoint = endpoint
        self.session = requests.Session()
        self.headers = {"User-Agent": "BDA501-Educational-Project"}

    def fetch_count(self, appid: int) -> int:
        max_attempts = int(os.getenv("PLAYER_COUNT_API_MAX_ATTEMPTS", "3"))
        for attempt in range(1, max_attempts + 1):
            try:
                response = self.session.get(
                    self.endpoint,
                    params={"appid": appid},
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
                body = payload.get("response")
                if not isinstance(body, Mapping) or body.get("result") != 1:
                    raise RuntimeError(
                        f"Steam player-count result is invalid for appid={appid}"
                    )
                value = body.get("player_count")
                if isinstance(value, bool):
                    raise RuntimeError(
                        f"Steam player_count is invalid for appid={appid}"
                    )
                player_count = int(value)
                if player_count < 0:
                    raise RuntimeError(
                        f"Steam player_count is negative for appid={appid}"
                    )
                return player_count
            except (
                requests.RequestException,
                RuntimeError,
                ValueError,
                TypeError,
            ):
                if attempt == max_attempts:
                    raise
                time.sleep(min(2**attempt, 30))
        raise RuntimeError(f"Steam request attempts exhausted for appid={appid}")


class PlayerCountProducerService:
    def __init__(
        self,
        *,
        state: PlayerCountState,
        client: PlayerCountClient,
        publisher: EventPublisher,
        clock: Callable[[], str] | None = None,
    ) -> None:
        self.state = state
        self.client = client
        self.publisher = publisher
        self.clock = clock or utc_now

    def poll_game(self, appid: int) -> PlayerCountPollResult:
        emitted = self._publish_pending(appid)
        player_count = self.client.fetch_count(appid)
        observed_at = self.clock()
        event = build_player_count_snapshot_event(
            appid,
            player_count,
            observed_at=observed_at,
            produced_at=self.clock(),
        )
        self.state.enqueue_event(event)
        emitted += self._publish_pending(appid)
        self.state.record_poll(
            appid,
            player_count=player_count,
            observed_at=event["event_time"],
            polled_at=self.clock(),
        )
        return PlayerCountPollResult(
            appid=appid,
            player_count=player_count,
            observed_at=event["event_time"],
            emitted=emitted,
        )

    def _publish_pending(self, appid: int) -> int:
        emitted = 0
        for event in self.state.pending_events(appid):
            self.publisher.send(appid, event)
            self.state.mark_published(
                str(event["event_id"]),
                published_at=self.clock(),
            )
            emitted += 1
        if emitted:
            self.publisher.flush()
        return emitted


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_active_scope(registry_path: Path):
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
    parser.add_argument("--once", action="store_true", help="run one poll cycle")
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY_PATH)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE_PATH)
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
    if args.max_games is not None and args.max_games <= 0:
        print("--max-games must be positive")
        return 2

    poll_interval = int(
        os.getenv("PLAYER_COUNT_POLL_INTERVAL_SECONDS", "600")
    )
    minimum_poll_interval = int(
        os.getenv("PLAYER_COUNT_MIN_POLL_INTERVAL_SECONDS", "300")
    )
    if not args.once and poll_interval < minimum_poll_interval:
        print(
            f"Safety stop: PLAYER_COUNT_POLL_INTERVAL_SECONDS={poll_interval} "
            "is below PLAYER_COUNT_MIN_POLL_INTERVAL_SECONDS="
            f"{minimum_poll_interval}"
        )
        return 2

    request_delay = float(
        os.getenv("PLAYER_COUNT_REQUEST_DELAY_SECONDS", "1.0")
    )
    max_active = int(os.getenv("PLAYER_COUNT_MAX_ACTIVE_GAMES", "100"))
    minimum_free_gb = float(os.getenv("STREAM_MIN_FREE_GB", "5"))
    publisher = KafkaEventPublisher(
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        ),
        topic=os.getenv("KAFKA_PLAYER_COUNT_TOPIC", "steam_player_events"),
        compression_type=os.getenv("KAFKA_COMPRESSION_TYPE", "gzip"),
    )
    client = SteamPlayerCountClient(
        os.getenv("STEAM_PLAYER_COUNT_URL", DEFAULT_ENDPOINT)
    )

    try:
        with PlayerCountState(args.state) as state:
            service = PlayerCountProducerService(
                state=state,
                client=client,
                publisher=publisher,
            )
            while True:
                ensure_disk_safety(PROJECT_ROOT, minimum_free_gb)
                active, counts = load_active_scope(args.registry)
                print(f"registry_total={counts['total']}")
                print(f"active_game_count={counts['active']}")
                print(f"retired_game_count={counts['retired']}")
                if counts["active"] > max_active:
                    print(
                        f"Safety stop: ACTIVE count {counts['active']} exceeds "
                        f"PLAYER_COUNT_MAX_ACTIVE_GAMES={max_active}"
                    )
                    return 2
                if args.max_games is not None:
                    active = active[: args.max_games]
                print(f"poll_scope_count={len(active)}")
                if not active:
                    print("No ACTIVE games; producer stopped cleanly")
                    return 0

                cycle_emitted = 0
                for index, entry in enumerate(active):
                    try:
                        result = service.poll_game(entry.appid)
                        cycle_emitted += result.emitted
                        print(
                            f"appid={entry.appid} "
                            f"player_count={result.player_count} "
                            f"observed_at={result.observed_at} "
                            f"emitted={result.emitted}"
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
