"""Dynamic validation for an isolated onboarding batch."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from src.common.jsonl import iter_jsonl
from src.onboarding.manifest import OnboardingManifest


@dataclass(frozen=True)
class BatchValidationReport:
    batch_id: str
    passed: bool
    expected_games: int
    metadata_records: int
    review_files: int
    expected_reviews: int
    total_reviews: int
    unique_review_ids: int
    baseline_overlap_count: int
    per_game_counts: dict[int, int]
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_staged_batch(
    manifest: OnboardingManifest,
    root: Path,
    *,
    baseline_reviews_root: Path | None = None,
) -> BatchValidationReport:
    """Validate staged metadata/reviews without changing any lifecycle state."""
    errors: list[str] = []
    warnings: list[str] = []
    expected_appids = set(manifest.appids)
    metadata_path = root / "games" / "games_raw.jsonl"
    reviews_root = root / "reviews_by_game"

    metadata_appids: set[int] = set()
    metadata_records = 0
    if not metadata_path.exists():
        errors.append(f"Missing staged metadata file: {metadata_path}")
    else:
        try:
            for row in iter_jsonl(metadata_path):
                metadata_records += 1
                try:
                    appid = int(row.get("appid"))
                except (TypeError, ValueError):
                    errors.append("Metadata record has invalid appid")
                    continue
                if appid in metadata_appids:
                    errors.append(f"Duplicate metadata appid: {appid}")
                metadata_appids.add(appid)
                if row.get("success") is not True:
                    errors.append(f"Metadata appid {appid} success is not true")
                data = row.get("data")
                if not isinstance(data, dict) or not str(data.get("name") or "").strip():
                    errors.append(f"Metadata appid {appid} has no data.name")
        except RuntimeError as exc:
            errors.append(str(exc))

    missing_metadata = sorted(expected_appids - metadata_appids)
    extra_metadata = sorted(metadata_appids - expected_appids)
    if missing_metadata:
        errors.append(f"Missing metadata appids: {missing_metadata}")
    if extra_metadata:
        errors.append(f"Unexpected metadata appids: {extra_metadata}")

    review_paths = sorted(reviews_root.glob("*.jsonl")) if reviews_root.exists() else []
    file_appids: set[int] = set()
    for path in review_paths:
        try:
            file_appids.add(int(path.stem))
        except ValueError:
            errors.append(f"Review filename is not an appid: {path.name}")
    missing_files = sorted(expected_appids - file_appids)
    extra_files = sorted(file_appids - expected_appids)
    if missing_files:
        errors.append(f"Missing review files for appids: {missing_files}")
    if extra_files:
        errors.append(f"Unexpected review files for appids: {extra_files}")

    baseline_ids = _load_baseline_ids(baseline_reviews_root, errors)
    batch_ids: set[str] = set()
    overlap_ids: set[str] = set()
    per_game_counts: dict[int, int] = {}
    total_reviews = 0

    for path in review_paths:
        try:
            file_appid = int(path.stem)
        except ValueError:
            continue
        local_ids: set[str] = set()
        try:
            rows = iter_jsonl(path)
            for row in rows:
                total_reviews += 1
                try:
                    row_appid = int(row.get("appid"))
                except (TypeError, ValueError):
                    errors.append(f"{path.name}: review has invalid appid")
                    continue
                if row_appid != file_appid:
                    errors.append(
                        f"{path.name}: row appid {row_appid} does not match filename"
                    )
                review = row.get("review")
                if not isinstance(review, dict):
                    errors.append(f"{path.name}: review payload is missing")
                    continue
                recommendationid = str(
                    review.get("recommendationid") or ""
                ).strip()
                if not recommendationid:
                    errors.append(f"{path.name}: missing recommendationid")
                    continue
                if recommendationid in local_ids:
                    errors.append(
                        f"{path.name}: duplicate recommendationid {recommendationid}"
                    )
                if recommendationid in batch_ids and recommendationid not in local_ids:
                    errors.append(
                        f"Cross-game duplicate recommendationid {recommendationid}"
                    )
                local_ids.add(recommendationid)
                batch_ids.add(recommendationid)
                if recommendationid in baseline_ids:
                    overlap_ids.add(recommendationid)
                if review.get("language") != "english":
                    errors.append(
                        f"{path.name}: recommendationid {recommendationid} is not English"
                    )
                if not isinstance(review.get("voted_up"), bool):
                    errors.append(
                        f"{path.name}: recommendationid {recommendationid} "
                        "has invalid voted_up"
                    )
            per_game_counts[file_appid] = len(local_ids)
        except RuntimeError as exc:
            errors.append(str(exc))

        actual = per_game_counts.get(file_appid, 0)
        if actual != manifest.target_reviews_per_game:
            errors.append(
                f"appid {file_appid}: expected {manifest.target_reviews_per_game} "
                f"unique reviews, found {actual}"
            )

    if overlap_ids:
        errors.append(
            f"Found {len(overlap_ids)} recommendation IDs already in baseline"
        )
    if total_reviews != manifest.expected_review_count:
        errors.append(
            f"Expected {manifest.expected_review_count} review rows, "
            f"found {total_reviews}"
        )
    if len(batch_ids) != manifest.expected_review_count:
        errors.append(
            f"Expected {manifest.expected_review_count} unique review IDs, "
            f"found {len(batch_ids)}"
        )

    return BatchValidationReport(
        batch_id=manifest.batch_id,
        passed=not errors,
        expected_games=manifest.expected_game_count,
        metadata_records=metadata_records,
        review_files=len(review_paths),
        expected_reviews=manifest.expected_review_count,
        total_reviews=total_reviews,
        unique_review_ids=len(batch_ids),
        baseline_overlap_count=len(overlap_ids),
        per_game_counts=dict(sorted(per_game_counts.items())),
        errors=tuple(errors),
        warnings=tuple(warnings),
    )


def save_validation_report(path: Path, report: BatchValidationReport) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f"{path.name}.tmp"
    temporary.write_text(
        json.dumps(report.to_dict(), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _load_baseline_ids(root: Path | None, errors: list[str]) -> set[str]:
    if root is None:
        return set()
    if not root.exists():
        errors.append(f"Baseline review root does not exist: {root}")
        return set()
    recommendationids: set[str] = set()
    for path in sorted(root.glob("*.jsonl")):
        try:
            for row in iter_jsonl(path):
                review = row.get("review")
                if not isinstance(review, dict):
                    continue
                recommendationid = str(
                    review.get("recommendationid") or ""
                ).strip()
                if recommendationid:
                    recommendationids.add(recommendationid)
        except RuntimeError as exc:
            errors.append(str(exc))
    return recommendationids
