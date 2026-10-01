"""Versioned onboarding manifests derived from the registry queue."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from src.discovery.policy import DiscoveryPolicy
from src.discovery.registry import STATUS_QUEUED, RegistryEntry


MANIFEST_SCHEMA_VERSION = 1
STATUS_PLANNED = "PLANNED"
BATCH_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{2,79}$")


@dataclass(frozen=True)
class OnboardingGame:
    appid: int
    name: str
    target_reviews: int
    source: str


@dataclass(frozen=True)
class OnboardingManifest:
    schema_version: int
    batch_id: str
    policy_version: int
    status: str
    created_at: str
    target_reviews_per_game: int
    games: tuple[OnboardingGame, ...]

    @property
    def expected_game_count(self) -> int:
        return len(self.games)

    @property
    def expected_review_count(self) -> int:
        return self.expected_game_count * self.target_reviews_per_game

    @property
    def appids(self) -> tuple[int, ...]:
        return tuple(game.appid for game in self.games)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["games"] = [asdict(game) for game in self.games]
        payload["expected_game_count"] = self.expected_game_count
        payload["expected_review_count"] = self.expected_review_count
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "OnboardingManifest":
        try:
            games = tuple(
                OnboardingGame(
                    appid=int(row["appid"]),
                    name=str(row["name"]),
                    target_reviews=int(row["target_reviews"]),
                    source=str(row["source"]),
                )
                for row in payload["games"]
            )
            manifest = cls(
                schema_version=int(payload["schema_version"]),
                batch_id=str(payload["batch_id"]),
                policy_version=int(payload["policy_version"]),
                status=str(payload["status"]),
                created_at=str(payload["created_at"]),
                target_reviews_per_game=int(payload["target_reviews_per_game"]),
                games=games,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("Invalid onboarding manifest") from exc
        _validate_manifest(manifest)
        return manifest


def build_manifest(
    batch_id: str,
    queue_rows: Iterable[Mapping[str, Any]],
    registry: Mapping[int, RegistryEntry],
    policy: DiscoveryPolicy,
    *,
    created_at: str | None = None,
) -> OnboardingManifest:
    """Build a deterministic manifest only from currently QUEUED entries."""
    if not BATCH_ID_PATTERN.fullmatch(batch_id):
        raise ValueError(
            "batch_id must be 3-80 lowercase letters, digits, '.', '_' or '-'"
        )

    target = policy.onboarding.target_reviews_per_game
    games: list[OnboardingGame] = []
    seen: set[int] = set()

    for row in queue_rows:
        try:
            appid = int(row["appid"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"Invalid queue row: {row}") from exc
        if appid <= 0:
            raise ValueError(f"Queue appid must be positive: {appid}")
        if appid in seen:
            raise ValueError(f"Duplicate queue appid: {appid}")
        seen.add(appid)

        entry = registry.get(appid)
        if entry is None:
            raise ValueError(f"Queue appid {appid} is missing from registry")
        if entry.status != STATUS_QUEUED:
            raise ValueError(
                f"Queue appid {appid} has registry status {entry.status}, "
                f"expected {STATUS_QUEUED}"
            )

        row_target = int(row.get("target_reviews", target))
        if row_target != target:
            raise ValueError(
                f"Queue appid {appid} target_reviews={row_target}, "
                f"policy requires {target}"
            )
        name = str(row.get("name") or entry.name or "").strip()
        if not name:
            raise ValueError(f"Queue appid {appid} has no game name")
        games.append(
            OnboardingGame(
                appid=appid,
                name=name,
                target_reviews=target,
                source=entry.source,
            )
        )

    if not games:
        raise ValueError("Onboarding queue is empty")
    if len(games) > policy.onboarding.max_new_games_per_cycle:
        raise ValueError(
            f"Queue contains {len(games)} games, policy permits at most "
            f"{policy.onboarding.max_new_games_per_cycle}"
        )

    manifest = OnboardingManifest(
        schema_version=MANIFEST_SCHEMA_VERSION,
        batch_id=batch_id,
        policy_version=policy.version,
        status=STATUS_PLANNED,
        created_at=created_at or datetime.now(timezone.utc).isoformat(),
        target_reviews_per_game=target,
        games=tuple(sorted(games, key=lambda game: game.appid)),
    )
    _validate_manifest(manifest)
    return manifest


def load_manifest(path: Path) -> OnboardingManifest:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Manifest does not exist: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Manifest is invalid JSON: {path}") from exc
    if not isinstance(payload, Mapping):
        raise ValueError(f"Manifest root must be an object: {path}")
    return OnboardingManifest.from_dict(payload)


def save_manifest(path: Path, manifest: OnboardingManifest) -> None:
    """Atomically create a manifest; refuse non-identical replacement."""
    payload = manifest.to_dict()
    if path.exists():
        existing = load_manifest(path)
        if existing != manifest:
            raise ValueError(f"Refusing to replace existing manifest: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f"{path.name}.tmp"
    temporary.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _validate_manifest(manifest: OnboardingManifest) -> None:
    if manifest.schema_version != MANIFEST_SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported manifest schema_version={manifest.schema_version}"
        )
    if not BATCH_ID_PATTERN.fullmatch(manifest.batch_id):
        raise ValueError(f"Invalid manifest batch_id: {manifest.batch_id}")
    if manifest.policy_version <= 0:
        raise ValueError("Manifest policy_version must be positive")
    if manifest.target_reviews_per_game <= 0:
        raise ValueError("Manifest target_reviews_per_game must be positive")
    if not manifest.games:
        raise ValueError("Manifest must contain at least one game")
    appids = manifest.appids
    if len(set(appids)) != len(appids):
        raise ValueError("Manifest contains duplicate appids")
    for game in manifest.games:
        if game.appid <= 0 or not game.name.strip():
            raise ValueError(f"Invalid manifest game: {game}")
        if game.target_reviews != manifest.target_reviews_per_game:
            raise ValueError(
                f"Manifest game {game.appid} target does not match batch target"
            )
