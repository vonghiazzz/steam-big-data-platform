"""Discovery Control Plane V1 orchestration without automatic backfill."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from ..common.jsonl import read_jsonl, write_jsonl
from .policy import (
    DEFAULT_POLICY_PATH,
    DiscoveryPolicy,
    GameQualificationInput,
    QualificationResult,
    evaluate_qualification,
    load_discovery_policy,
)
from .registry import (
    OnboardingPlan,
    RegistryEntry,
    build_onboarding_plan,
    load_registry,
    reconcile_registry,
    save_registry,
    seed_registry_from_snapshot,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CATALOG_ROOT = PROJECT_ROOT / "data" / "raw" / "catalog_probe"
DEFAULT_CANDIDATES_PATH = CATALOG_ROOT / "candidates.jsonl"
DEFAULT_METADATA_PATH = CATALOG_ROOT / "metadata_probe_raw.jsonl"
DEFAULT_REVIEW_PROBE_PATH = CATALOG_ROOT / "review_probe.jsonl"
DEFAULT_SNAPSHOT_PATH = PROJECT_ROOT / "data" / "raw" / "selected_50_games.jsonl"
DEFAULT_REGISTRY_ROOT = PROJECT_ROOT / "data" / "raw" / "registry"
DEFAULT_REGISTRY_PATH = DEFAULT_REGISTRY_ROOT / "game_registry.jsonl"
DEFAULT_PLAN_PATH = DEFAULT_REGISTRY_ROOT / "onboarding_plan.json"
DEFAULT_QUEUE_PATH = DEFAULT_REGISTRY_ROOT / "onboarding_queue.jsonl"
DEFAULT_REPORT_PATH = DEFAULT_REGISTRY_ROOT / "discovery_run_report.json"

Evaluator = Callable[..., QualificationResult]


@dataclass(frozen=True)
class QualificationBatch:
    candidate_count: int
    duplicate_candidate_count: int
    qualified_candidates: tuple[dict[str, Any], ...]
    rejected_candidates: tuple[dict[str, Any], ...]
    failed_candidates: tuple[dict[str, Any], ...]
    rejection_reason_counts: dict[str, int]

    @property
    def qualified_count(self) -> int:
        return len(self.qualified_candidates)

    @property
    def rejected_count(self) -> int:
        return len(self.rejected_candidates)

    @property
    def failed_count(self) -> int:
        return len(self.failed_candidates)


@dataclass(frozen=True)
class DiscoveryRunReport:
    candidate_count: int
    duplicate_candidate_count: int
    qualified_count: int
    rejected_count: int
    failed_count: int
    existing_active_count: int
    new_count: int
    queued_count: int
    deferred_count: int
    policy_version: int
    rejection_reason_counts: dict[str, int]

    def __post_init__(self) -> None:
        processed = self.qualified_count + self.rejected_count + self.failed_count
        if processed != self.candidate_count:
            raise ValueError(
                "Discovery report does not reconcile: "
                f"candidate_count={self.candidate_count}, processed={processed}"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_count": self.candidate_count,
            "duplicate_candidate_count": self.duplicate_candidate_count,
            "qualified_count": self.qualified_count,
            "rejected_count": self.rejected_count,
            "failed_count": self.failed_count,
            "existing_active_count": self.existing_active_count,
            "new_count": self.new_count,
            "queued_count": self.queued_count,
            "deferred_count": self.deferred_count,
            "policy_version": self.policy_version,
            "rejection_reason_counts": dict(
                sorted(self.rejection_reason_counts.items())
            ),
        }


@dataclass(frozen=True)
class DiscoveryControlPlaneResult:
    registry: dict[int, RegistryEntry]
    qualification: QualificationBatch
    onboarding_plan: OnboardingPlan
    run_report: DiscoveryRunReport


def build_candidate_evidence(
    candidate_rows: Iterable[Mapping[str, Any]],
    metadata_rows: Iterable[Mapping[str, Any]],
    review_rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    metadata_by_appid = _index_first_by_appid(metadata_rows)
    review_by_appid = _index_first_by_appid(review_rows)
    evidence: list[dict[str, Any]] = []

    for candidate in candidate_rows:
        candidate_copy = dict(candidate)
        raw_appid = candidate_copy.get("appid")
        try:
            appid = _positive_appid(raw_appid)
        except ValueError:
            evidence.append(candidate_copy)
            continue

        metadata_record = metadata_by_appid.get(appid)
        metadata_data = None
        if metadata_record and metadata_record.get("success") is True:
            possible_data = metadata_record.get("data")
            if isinstance(possible_data, Mapping):
                metadata_data = possible_data

        review_record = review_by_appid.get(appid)
        query_summary: Mapping[str, Any] = {}
        if review_record:
            possible_summary = review_record.get("query_summary")
            if isinstance(possible_summary, Mapping):
                query_summary = possible_summary

        release_info: Mapping[str, Any] = {}
        if metadata_data:
            possible_release = metadata_data.get("release_date")
            if isinstance(possible_release, Mapping):
                release_info = possible_release

        evidence.append(
            {
                **candidate_copy,
                "appid": appid,
                "name": (
                    metadata_data.get("name")
                    if metadata_data
                    else candidate_copy.get("title")
                ),
                "app_type": (
                    metadata_data.get("type")
                    if metadata_data
                    else None
                ),
                "release_date": release_info.get("date"),
                "total_reviews": query_summary.get("total_reviews"),
                "metadata_available": metadata_data is not None,
                "review_endpoint_available": _review_endpoint_available(
                    review_record
                ),
            }
        )

    return evidence


def qualify_candidates(
    candidate_evidence: Iterable[Mapping[str, Any]],
    policy: DiscoveryPolicy,
    *,
    current_date: date | None = None,
    evaluator: Evaluator = evaluate_qualification,
) -> QualificationBatch:
    unique_candidates: dict[int, Mapping[str, Any]] = {}
    invalid_candidates: list[Mapping[str, Any]] = []
    duplicate_count = 0

    for candidate in candidate_evidence:
        try:
            appid = _positive_appid(candidate.get("appid"))
        except Exception:
            invalid_candidates.append(candidate)
            continue
        if appid in unique_candidates:
            duplicate_count += 1
            continue
        unique_candidates[appid] = candidate

    qualified: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = [
        {
            "appid": candidate.get("appid"),
            "error_code": "INVALID_CANDIDATE",
            "error_message": "Candidate has a missing or invalid appid",
        }
        for candidate in invalid_candidates
    ]
    reason_counts: Counter[str] = Counter()

    for appid in sorted(unique_candidates):
        candidate = unique_candidates[appid]
        try:
            result = evaluator(
                GameQualificationInput(
                    appid=appid,
                    app_type=candidate.get("app_type"),
                    release_date=candidate.get("release_date"),
                    total_reviews=candidate.get("total_reviews"),
                    metadata_available=(
                        candidate.get("metadata_available") is True
                    ),
                    review_endpoint_available=(
                        candidate.get("review_endpoint_available") is True
                    ),
                    playtime_minutes=candidate.get("playtime_minutes"),
                ),
                policy.qualification,
                policy_version=policy.version,
                current_date=current_date,
            )
        except Exception as exc:
            failed.append(
                {
                    "appid": appid,
                    "error_code": "CANDIDATE_PROCESSING_ERROR",
                    "error_message": str(exc),
                }
            )
            continue

        output = {
            **dict(candidate),
            "appid": appid,
            "qualification": result.to_dict(),
        }
        if result.qualified:
            qualified.append(output)
        else:
            rejected.append(output)
            reason_counts.update(reason.rule for reason in result.reasons)

    return QualificationBatch(
        candidate_count=len(unique_candidates) + len(invalid_candidates),
        duplicate_candidate_count=duplicate_count,
        qualified_candidates=tuple(qualified),
        rejected_candidates=tuple(rejected),
        failed_candidates=tuple(failed),
        rejection_reason_counts=dict(sorted(reason_counts.items())),
    )


def run_control_plane(
    candidate_evidence: Iterable[Mapping[str, Any]],
    snapshot_rows: Iterable[Mapping[str, Any]],
    registry: Mapping[int, RegistryEntry],
    policy: DiscoveryPolicy,
    *,
    current_date: date | None = None,
    run_timestamp: str | None = None,
    evaluator: Evaluator = evaluate_qualification,
) -> DiscoveryControlPlaneResult:
    qualification = qualify_candidates(
        candidate_evidence,
        policy,
        current_date=current_date,
        evaluator=evaluator,
    )
    seeded_registry = seed_registry_from_snapshot(
        snapshot_rows,
        registry,
        policy_version=policy.version,
    )
    reconciliation = reconcile_registry(
        qualification.qualified_candidates,
        seeded_registry,
        policy_version=policy.version,
        run_timestamp=run_timestamp,
    )
    planned_registry, plan = build_onboarding_plan(
        reconciliation,
        qualification.qualified_candidates,
        policy.onboarding,
        policy_version=policy.version,
        run_timestamp=run_timestamp,
    )
    report = DiscoveryRunReport(
        candidate_count=qualification.candidate_count,
        duplicate_candidate_count=qualification.duplicate_candidate_count,
        qualified_count=qualification.qualified_count,
        rejected_count=qualification.rejected_count,
        failed_count=qualification.failed_count,
        existing_active_count=plan.existing_active_count,
        new_count=plan.new_count,
        queued_count=plan.queued_count,
        deferred_count=plan.deferred_count,
        policy_version=policy.version,
        rejection_reason_counts=qualification.rejection_reason_counts,
    )
    return DiscoveryControlPlaneResult(
        registry=planned_registry,
        qualification=qualification,
        onboarding_plan=plan,
        run_report=report,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--policy-path", type=Path, default=DEFAULT_POLICY_PATH)
    parser.add_argument(
        "--candidates-path", type=Path, default=DEFAULT_CANDIDATES_PATH
    )
    parser.add_argument(
        "--metadata-path", type=Path, default=DEFAULT_METADATA_PATH
    )
    parser.add_argument(
        "--review-probe-path", type=Path, default=DEFAULT_REVIEW_PROBE_PATH
    )
    parser.add_argument(
        "--snapshot-path", type=Path, default=DEFAULT_SNAPSHOT_PATH
    )
    parser.add_argument(
        "--registry-path", type=Path, default=DEFAULT_REGISTRY_PATH
    )
    parser.add_argument("--plan-path", type=Path, default=DEFAULT_PLAN_PATH)
    parser.add_argument("--queue-path", type=Path, default=DEFAULT_QUEUE_PATH)
    parser.add_argument("--report-path", type=Path, default=DEFAULT_REPORT_PATH)
    parser.add_argument(
        "--as-of-date",
        type=date.fromisoformat,
        default=None,
        help="Optional YYYY-MM-DD date for reproducible release-age evaluation",
    )
    args = parser.parse_args()

    policy = load_discovery_policy(args.policy_path)
    candidate_evidence = build_candidate_evidence(
        read_jsonl(args.candidates_path),
        read_jsonl(args.metadata_path),
        read_jsonl(args.review_probe_path),
    )
    run_timestamp = datetime.now(timezone.utc).isoformat()
    result = run_control_plane(
        candidate_evidence,
        read_jsonl(args.snapshot_path),
        load_registry(args.registry_path),
        policy,
        current_date=args.as_of_date,
        run_timestamp=run_timestamp,
    )

    save_registry(args.registry_path, result.registry)
    _write_json(args.plan_path, result.onboarding_plan.to_dict())
    write_jsonl(args.queue_path, result.onboarding_plan.queued_games)
    _write_json(args.report_path, result.run_report.to_dict())

    print(json.dumps(result.run_report.to_dict(), indent=2, ensure_ascii=False))
    print(f"Registry: {args.registry_path}")
    print(f"Onboarding plan: {args.plan_path}")
    print(f"Crawler queue: {args.queue_path}")
    print(f"Run report: {args.report_path}")
    print("Historical backfill was not started.")


def _index_first_by_appid(
    rows: Iterable[Mapping[str, Any]],
) -> dict[int, Mapping[str, Any]]:
    indexed: dict[int, Mapping[str, Any]] = {}
    for row in rows:
        try:
            appid = _positive_appid(row.get("appid"))
        except ValueError:
            continue
        indexed.setdefault(appid, row)
    return indexed


def _review_endpoint_available(row: Mapping[str, Any] | None) -> bool:
    if row is None:
        return False
    qualification = row.get("qualification")
    if isinstance(qualification, Mapping):
        reasons = qualification.get("reasons", [])
        if isinstance(reasons, list):
            for reason in reasons:
                if (
                    isinstance(reason, Mapping)
                    and reason.get("rule") == "REVIEW_ENDPOINT_UNAVAILABLE"
                ):
                    return False
    summary = row.get("query_summary")
    return isinstance(summary, Mapping) and bool(summary)


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


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.parent / f"{path.name}.tmp"
    temporary_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temporary_path.replace(path)


if __name__ == "__main__":
    main()
