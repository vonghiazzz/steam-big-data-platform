"""Pure activation gate; persistence is intentionally a later explicit step."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Mapping

from src.discovery.registry import STATUS_ACTIVE, STATUS_QUEUED, RegistryEntry
from src.onboarding.manifest import OnboardingManifest
from src.onboarding.validation import BatchValidationReport


@dataclass(frozen=True)
class GameReadiness:
    appid: int
    bronze_metadata: bool
    bronze_reviews: int
    silver_game: bool
    silver_reviews: int
    gold_reviews: int
    mongo_game_metrics: bool


def build_activated_registry(
    registry: Mapping[int, RegistryEntry],
    manifest: OnboardingManifest,
    validation: BatchValidationReport,
    readiness: Mapping[int, GameReadiness],
    *,
    activated_at: str,
) -> dict[int, RegistryEntry]:
    """Return an activated copy only when every materialization gate passes."""
    if not validation.passed or validation.batch_id != manifest.batch_id:
        raise ValueError("Batch validation has not passed for this manifest")
    updated = dict(registry)
    failures: list[str] = []

    for game in manifest.games:
        entry = updated.get(game.appid)
        if entry is None or entry.status != STATUS_QUEUED:
            failures.append(
                f"appid {game.appid}: registry status is not {STATUS_QUEUED}"
            )
            continue
        state = readiness.get(game.appid)
        if state is None:
            failures.append(f"appid {game.appid}: readiness evidence is missing")
            continue
        if state.appid != game.appid:
            failures.append(f"appid {game.appid}: readiness appid mismatch")
            continue
        expected = game.target_reviews
        checks = {
            "bronze_metadata": state.bronze_metadata,
            "bronze_reviews": state.bronze_reviews == expected,
            "silver_game": state.silver_game,
            "silver_reviews": state.silver_reviews == expected,
            "gold_reviews": state.gold_reviews == expected,
            "mongo_game_metrics": state.mongo_game_metrics,
        }
        failed_checks = sorted(name for name, passed in checks.items() if not passed)
        if failed_checks:
            failures.append(
                f"appid {game.appid}: failed readiness checks {failed_checks}"
            )

    if failures:
        raise ValueError("Activation refused: " + "; ".join(failures))

    for game in manifest.games:
        updated[game.appid] = replace(
            updated[game.appid],
            status=STATUS_ACTIVE,
            activated_at=activated_at,
            last_error_code=None,
            last_error_message=None,
        )
    return updated
