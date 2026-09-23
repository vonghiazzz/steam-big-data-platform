"""Lightweight JSONL game registry and deterministic onboarding planning."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Iterable, Mapping

from ..common.jsonl import read_jsonl, write_jsonl
from .policy import OnboardingPolicy


STATUS_NEW = "NEW"
STATUS_QUEUED = "QUEUED"
STATUS_ACTIVE = "ACTIVE"
STATUS_FAILED = "FAILED"
STATUS_PAUSED = "PAUSED"
STATUS_RETIRED = "RETIRED"

SUPPORTED_STATUSES = {
    STATUS_NEW,
    STATUS_QUEUED,
    STATUS_ACTIVE,
    STATUS_FAILED,
    STATUS_PAUSED,
    STATUS_RETIRED,
}

SOURCE_INITIAL_RESEARCH_SNAPSHOT = "INITIAL_RESEARCH_SNAPSHOT"
SOURCE_DISCOVERY = "DISCOVERY"


@dataclass(frozen=True)
class RegistryEntry:
    appid: int
    status: str
    source: str
    policy_version: int
    name: str | None = None
    discovered_at: str | None = None
    qualified_at: str | None = None
    queued_at: str | None = None
    activated_at: str | None = None
    last_polled_at: str | None = None
    failure_count: int = 0
    last_error_code: str | None = None
    last_error_message: str | None = None

    def __post_init__(self) -> None:
        if self.appid <= 0:
            raise ValueError("Registry appid must be a positive integer")
        if self.status not in SUPPORTED_STATUSES:
            raise ValueError(f"Unsupported registry status: {self.status}")
        if not self.source:
            raise ValueError("Registry source must not be empty")
        if self.policy_version <= 0:
            raise ValueError("Registry policy_version must be positive")
        if self.failure_count < 0:
            raise ValueError("Registry failure_count must not be negative")

    @classmethod
    def from_dict(cls, row: Mapping[str, Any]) -> "RegistryEntry":
        try:
            appid = int(row["appid"])
            status = str(row["status"])
            source = str(row["source"])
            policy_version = int(row["policy_version"])
            failure_count = int(row.get("failure_count", 0))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"Invalid registry row: {row}") from exc

        return cls(
            appid=appid,
            status=status,
            source=source,
            policy_version=policy_version,
            name=_optional_string(row.get("name")),
            discovered_at=_optional_string(row.get("discovered_at")),
            qualified_at=_optional_string(row.get("qualified_at")),
            queued_at=_optional_string(row.get("queued_at")),
            activated_at=_optional_string(row.get("activated_at")),
            last_polled_at=_optional_string(row.get("last_polled_at")),
            failure_count=failure_count,
            last_error_code=_optional_string(row.get("last_error_code")),
            last_error_message=_optional_string(
                row.get("last_error_message")
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in asdict(self).items()
            if value is not None
        }


@dataclass(frozen=True)
class RegistryReconciliation:
    registry: dict[int, RegistryEntry]
    existing_active_appids: tuple[int, ...]
    existing_other_appids: tuple[int, ...]
    newly_discovered_appids: tuple[int, ...]
    pending_new_appids: tuple[int, ...]


@dataclass(frozen=True)
class OnboardingPlan:
    policy_version: int
    target_reviews_per_game: int
    qualified_count: int
    existing_active_count: int
    new_count: int
    queued_count: int
    deferred_count: int
    queued_appids: tuple[int, ...]
    deferred_appids: tuple[int, ...]
    queued_games: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy_version": self.policy_version,
            "target_reviews_per_game": self.target_reviews_per_game,
            "qualified_count": self.qualified_count,
            "existing_active_count": self.existing_active_count,
            "new_count": self.new_count,
            "queued_count": self.queued_count,
            "deferred_count": self.deferred_count,
            "queued_appids": list(self.queued_appids),
            "deferred_appids": list(self.deferred_appids),
            "queued_games": list(self.queued_games),
        }


def load_registry(path: Path) -> dict[int, RegistryEntry]:
    if not path.exists():
        return {}

    registry: dict[int, RegistryEntry] = {}
    for row in read_jsonl(path):
        entry = RegistryEntry.from_dict(row)
        if entry.appid in registry:
            raise ValueError(
                f"Duplicate appid {entry.appid} in registry {path}"
            )
        registry[entry.appid] = entry
    return registry


def save_registry(path: Path, registry: Mapping[int, RegistryEntry]) -> None:
    rows = [registry[appid].to_dict() for appid in sorted(registry)]
    write_jsonl(path, rows)


def seed_registry_from_snapshot(
    snapshot_rows: Iterable[Mapping[str, Any]],
    registry: Mapping[int, RegistryEntry],
    *,
    policy_version: int,
) -> dict[int, RegistryEntry]:
    seeded = dict(registry)
    for row in snapshot_rows:
        appid = _positive_appid(row.get("appid"))
        if appid in seeded:
            continue
        seeded[appid] = RegistryEntry(
            appid=appid,
            name=_optional_string(row.get("name")),
            status=STATUS_ACTIVE,
            source=SOURCE_INITIAL_RESEARCH_SNAPSHOT,
            policy_version=policy_version,
        )
    return seeded


def reconcile_registry(
    qualified_candidates: Iterable[Mapping[str, Any]],
    registry: Mapping[int, RegistryEntry],
    *,
    policy_version: int,
    run_timestamp: str | None = None,
) -> RegistryReconciliation:
    reconciled = dict(registry)
    candidates: dict[int, Mapping[str, Any]] = {}
    for candidate in qualified_candidates:
        appid = _positive_appid(candidate.get("appid"))
        candidates.setdefault(appid, candidate)

    existing_active: list[int] = []
    existing_other: list[int] = []
    newly_discovered: list[int] = []

    for appid in sorted(candidates):
        existing = reconciled.get(appid)
        if existing is not None:
            if existing.status == STATUS_ACTIVE:
                existing_active.append(appid)
            else:
                existing_other.append(appid)
                if existing.status == STATUS_NEW:
                    reconciled[appid] = replace(
                        existing,
                        policy_version=policy_version,
                        qualified_at=run_timestamp or existing.qualified_at,
                    )
            continue

        candidate = candidates[appid]
        reconciled[appid] = RegistryEntry(
            appid=appid,
            name=_optional_string(candidate.get("name")),
            status=STATUS_NEW,
            source=SOURCE_DISCOVERY,
            policy_version=policy_version,
            discovered_at=run_timestamp,
            qualified_at=run_timestamp,
        )
        newly_discovered.append(appid)

    pending_new = [
        appid
        for appid in sorted(candidates)
        if reconciled[appid].status == STATUS_NEW
    ]

    return RegistryReconciliation(
        registry=reconciled,
        existing_active_appids=tuple(existing_active),
        existing_other_appids=tuple(existing_other),
        newly_discovered_appids=tuple(newly_discovered),
        pending_new_appids=tuple(pending_new),
    )


def build_onboarding_plan(
    reconciliation: RegistryReconciliation,
    qualified_candidates: Iterable[Mapping[str, Any]],
    onboarding_policy: OnboardingPolicy,
    *,
    policy_version: int,
    run_timestamp: str | None = None,
) -> tuple[dict[int, RegistryEntry], OnboardingPlan]:
    candidates: dict[int, Mapping[str, Any]] = {}
    for candidate in qualified_candidates:
        appid = _positive_appid(candidate.get("appid"))
        candidates.setdefault(appid, candidate)

    pending = sorted(reconciliation.pending_new_appids)
    queued = pending[: onboarding_policy.max_new_games_per_cycle]
    deferred = pending[onboarding_policy.max_new_games_per_cycle :]

    registry = dict(reconciliation.registry)
    for appid in queued:
        registry[appid] = replace(
            registry[appid],
            status=STATUS_QUEUED,
            queued_at=run_timestamp,
        )

    queued_games = tuple(
        {
            "appid": appid,
            "name": candidates[appid].get("name"),
            "target_reviews": onboarding_policy.target_reviews_per_game,
            "policy_version": policy_version,
            "source": registry[appid].source,
        }
        for appid in queued
    )

    plan = OnboardingPlan(
        policy_version=policy_version,
        target_reviews_per_game=onboarding_policy.target_reviews_per_game,
        qualified_count=len(candidates),
        existing_active_count=len(
            reconciliation.existing_active_appids
        ),
        new_count=len(pending),
        queued_count=len(queued),
        deferred_count=len(deferred),
        queued_appids=tuple(queued),
        deferred_appids=tuple(deferred),
        queued_games=queued_games,
    )
    return registry, plan


def _positive_appid(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError(f"Invalid appid: {value}")
    try:
        appid = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid appid: {value}") from exc
    if appid <= 0:
        raise ValueError(f"Invalid appid: {value}")
    return appid


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
