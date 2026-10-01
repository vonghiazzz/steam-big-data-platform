"""Bounded, staged Steam catalog refresh for discovery evidence."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Mapping

from ..common.jsonl import read_jsonl
from .catalog_probe import probe_catalog
from .catalog_qualify import qualify_catalog
from .paths import (
    CANDIDATES_PATH,
    ELIGIBLE_GAMES_PATH,
    METADATA_PROBE_PATH,
    REGISTRY_PATH,
    REVIEW_PROBE_PATH,
)
from .policy import DEFAULT_POLICY_PATH, DiscoveryPolicy, load_discovery_policy
from .registry import load_registry
from .review_qualify import qualify_reviews


@dataclass(frozen=True)
class CatalogRefreshResult:
    page_size: int
    max_pages: int
    candidate_count: int
    metadata_count: int
    eligible_count: int
    review_probe_count: int
    known_candidate_count: int
    unseen_appids: tuple[int, ...]

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["unseen_appids"] = list(self.unseen_appids)
        return payload


def refresh_catalog(
    *,
    policy: DiscoveryPolicy,
    policy_path: Path,
    registry_path: Path,
    candidates_path: Path,
    metadata_path: Path,
    eligible_path: Path,
    review_probe_path: Path,
    page_size: int | None = None,
    max_pages: int | None = None,
    delay: float | None = None,
) -> CatalogRefreshResult:
    """Acquire into staging and promote evidence only after every stage passes."""
    refresh_policy = policy.catalog_refresh
    if not refresh_policy.enabled:
        raise ValueError("Catalog refresh is disabled by discovery policy")

    effective_page_size = _bounded_positive_override(
        page_size, refresh_policy.page_size, "page_size"
    )
    effective_max_pages = _bounded_positive_override(
        max_pages, refresh_policy.max_pages, "max_pages"
    )
    effective_delay = (
        refresh_policy.request_delay_seconds if delay is None else float(delay)
    )
    if effective_delay < 0:
        raise ValueError("delay must be non-negative")

    staging_parent = candidates_path.parent
    staging_parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".catalog-refresh-", dir=staging_parent
    ) as directory:
        staging = Path(directory)
        staged_candidates = staging / "candidates.jsonl"
        staged_metadata = staging / "metadata_probe_raw.jsonl"
        staged_eligible = staging / "eligible_games.jsonl"
        staged_reviews = staging / "review_probe.jsonl"

        probe_catalog(
            staging,
            page_size=effective_page_size,
            max_pages=effective_max_pages,
            delay=effective_delay,
        )
        qualify_catalog(staged_candidates, staging, effective_delay)
        qualify_reviews(
            staged_eligible,
            staged_reviews,
            effective_delay,
            policy_path=policy_path,
        )

        counts = validate_staged_evidence(
            staged_candidates,
            staged_metadata,
            staged_eligible,
            staged_reviews,
        )
        candidate_ids = _unique_appids(read_jsonl(staged_candidates), "candidates")
        registry_ids = set(load_registry(registry_path))

        _promote_files(
            {
                staged_candidates: candidates_path,
                staged_metadata: metadata_path,
                staged_eligible: eligible_path,
                staged_reviews: review_probe_path,
            }
        )

    unseen = tuple(sorted(candidate_ids - registry_ids))
    return CatalogRefreshResult(
        page_size=effective_page_size,
        max_pages=effective_max_pages,
        candidate_count=counts["candidate_count"],
        metadata_count=counts["metadata_count"],
        eligible_count=counts["eligible_count"],
        review_probe_count=counts["review_probe_count"],
        known_candidate_count=len(candidate_ids & registry_ids),
        unseen_appids=unseen,
    )


def validate_staged_evidence(
    candidates_path: Path,
    metadata_path: Path,
    eligible_path: Path,
    review_probe_path: Path,
) -> dict[str, int]:
    candidates = read_jsonl(candidates_path)
    metadata = read_jsonl(metadata_path)
    eligible = read_jsonl(eligible_path)
    reviews = read_jsonl(review_probe_path)

    candidate_ids = _unique_appids(candidates, "candidates")
    metadata_ids = _unique_appids(metadata, "metadata probe")
    eligible_ids = _unique_appids(eligible, "eligible games")
    review_ids = _unique_appids(reviews, "review probe")

    if not candidate_ids:
        raise ValueError("Catalog refresh returned no candidate AppIDs")
    if metadata_ids != candidate_ids:
        raise ValueError(
            "Metadata probe AppIDs do not exactly match catalog candidates"
        )
    if not eligible_ids.issubset(candidate_ids):
        raise ValueError("Eligible-game AppIDs are not a candidate subset")
    if review_ids != eligible_ids:
        raise ValueError(
            "Review probe AppIDs do not exactly match metadata-eligible games"
        )

    return {
        "candidate_count": len(candidate_ids),
        "metadata_count": len(metadata_ids),
        "eligible_count": len(eligible_ids),
        "review_probe_count": len(review_ids),
    }


def _unique_appids(rows: Iterable[Mapping], label: str) -> set[int]:
    appids: set[int] = set()
    row_count = 0
    for row in rows:
        row_count += 1
        value = row.get("appid")
        if isinstance(value, bool):
            raise ValueError(f"Invalid AppID in {label}: {value}")
        try:
            appid = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid AppID in {label}: {value}") from exc
        if appid <= 0:
            raise ValueError(f"Invalid AppID in {label}: {value}")
        if appid in appids:
            raise ValueError(f"Duplicate AppID {appid} in {label}")
        appids.add(appid)
    if len(appids) != row_count:
        raise ValueError(f"Duplicate AppIDs in {label}")
    return appids


def _bounded_positive_override(
    value: int | None, configured_maximum: int, field: str
) -> int:
    effective = configured_maximum if value is None else value
    if isinstance(effective, bool) or effective <= 0:
        raise ValueError(f"{field} must be a positive integer")
    if effective > configured_maximum:
        raise ValueError(
            f"{field}={effective} exceeds configured bound {configured_maximum}"
        )
    return effective


def _promote_files(files: Mapping[Path, Path]) -> None:
    """Prepare every destination, then replace it with rollback on failure."""
    token = uuid.uuid4().hex
    prepared: dict[Path, Path] = {}
    backups: dict[Path, Path] = {}
    promoted: list[Path] = []
    try:
        for source, destination in files.items():
            destination.parent.mkdir(parents=True, exist_ok=True)
            temporary = destination.parent / f".{destination.name}.{token}.tmp"
            shutil.copyfile(source, temporary)
            prepared[destination] = temporary
            if destination.exists():
                backup = destination.parent / f".{destination.name}.{token}.bak"
                shutil.copyfile(destination, backup)
                backups[destination] = backup

        for destination, temporary in prepared.items():
            os.replace(temporary, destination)
            promoted.append(destination)
    except Exception:
        for destination in promoted:
            backup = backups.get(destination)
            if backup and backup.exists():
                os.replace(backup, destination)
            elif destination.exists():
                destination.unlink()
        raise
    finally:
        for temporary in prepared.values():
            if temporary.exists():
                temporary.unlink()
        for backup in backups.values():
            if backup.exists():
                backup.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy-path", type=Path, default=DEFAULT_POLICY_PATH)
    parser.add_argument("--registry-path", type=Path, default=REGISTRY_PATH)
    parser.add_argument("--candidates-path", type=Path, default=CANDIDATES_PATH)
    parser.add_argument("--metadata-path", type=Path, default=METADATA_PROBE_PATH)
    parser.add_argument("--eligible-path", type=Path, default=ELIGIBLE_GAMES_PATH)
    parser.add_argument("--review-probe-path", type=Path, default=REVIEW_PROBE_PATH)
    parser.add_argument("--page-size", type=int, default=None)
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--delay", type=float, default=None)
    args = parser.parse_args()

    result = refresh_catalog(
        policy=load_discovery_policy(args.policy_path),
        policy_path=args.policy_path,
        registry_path=args.registry_path,
        candidates_path=args.candidates_path,
        metadata_path=args.metadata_path,
        eligible_path=args.eligible_path,
        review_probe_path=args.review_probe_path,
        page_size=args.page_size,
        max_pages=args.max_pages,
        delay=args.delay,
    )
    print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
