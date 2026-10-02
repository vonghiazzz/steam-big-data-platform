"""Discovery Control Plane V1 orchestration without automatic backfill."""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping
import requests
from .registry import (
    RegistryEntry,
    build_onboarding_plan,
    calculate_onboarding_capacity,
    reconcile_registry,
    seed_registry_from_snapshot,
)
from .feasibility_cache import (
    load_feasibility_cache,
    save_feasibility_cache,
    snapshot_fingerprint,
)

from .review_feasibility import (
    probe_review_feasibility,
)

from ..common.jsonl import read_jsonl, write_jsonl
from .policy import (
    DEFAULT_POLICY_PATH,
    DiscoveryPolicy,
    GameQualificationInput,
    QualificationResult,
    evaluate_qualification,
    load_discovery_policy,
)
from .paths import (
    CANDIDATES_PATH,
    CATALOG_ROOT,
    DISCOVERY_REPORT_PATH,
    ELIGIBLE_GAMES_PATH,
    INITIAL_SNAPSHOT_PATH,
    METADATA_PROBE_PATH,
    ONBOARDING_PLAN_PATH,
    ONBOARDING_QUEUE_PATH,
    PROJECT_ROOT,
    REGISTRY_PATH,
    REGISTRY_ROOT,
    REVIEW_PROBE_PATH,
)
from .refresh_catalog import refresh_catalog
from .registry import (
    OnboardingPlan,
    RegistryEntry,
    build_onboarding_plan,
    load_registry,
    reconcile_registry,
    reconcile_onboarding_queue,
    save_registry,
    seed_registry_from_snapshot,
)


DEFAULT_CANDIDATES_PATH = CANDIDATES_PATH
DEFAULT_METADATA_PATH = METADATA_PROBE_PATH
DEFAULT_REVIEW_PROBE_PATH = REVIEW_PROBE_PATH
DEFAULT_SNAPSHOT_PATH = INITIAL_SNAPSHOT_PATH
DEFAULT_REGISTRY_ROOT = REGISTRY_ROOT
DEFAULT_REGISTRY_PATH = REGISTRY_PATH
DEFAULT_PLAN_PATH = ONBOARDING_PLAN_PATH
DEFAULT_QUEUE_PATH = ONBOARDING_QUEUE_PATH
DEFAULT_REPORT_PATH = DISCOVERY_REPORT_PATH
DEFAULT_FEASIBILITY_CACHE_PATH = (
    PROJECT_ROOT
    / "data"
    / "state"
    / "discovery"
    / "crawl_feasibility_cache_v1.json"
)

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

def filter_crawl_feasible_candidates(
    candidates,
    target_reviews: int,
    *,
    delay: float = 0.0,
    session=None,
    feasibility_probe=probe_review_feasibility,
):
    """
    Keep only candidates that can satisfy the historical
    crawl contract.

    Basic qualification and crawl feasibility are separate:
    - qualification checks game/source eligibility
    - feasibility checks whether onboarding can obtain the
      required number of unique English reviews
    """

    if target_reviews <= 0:
        raise ValueError(
            "target_reviews must be positive"
        )

    own_session = session is None

    if session is None:
        session = requests.Session()

    feasible = []
    rejected = []

    try:
        for candidate in candidates:
            appid = int(candidate["appid"])

            result = feasibility_probe(
                session,
                appid,
                target_reviews,
                delay=delay,
            )

            enriched = {
                **dict(candidate),
                "crawl_feasibility": (
                    result.to_dict()
                ),
            }

            if result.feasible:
                feasible.append(enriched)

                print(
                    "CRAWL FEASIBLE:",
                    appid,
                    f"{result.unique_reviews}/"
                    f"{target_reviews}",
                )
            else:
                rejected.append(enriched)

                print(
                    "CRAWL REJECTED:",
                    appid,
                    f"{result.unique_reviews}/"
                    f"{target_reviews}",
                    result.status,
                )

    finally:
        if own_session:
            session.close()

    return feasible, rejected

def apply_crawl_feasibility(
    qualification: QualificationBatch,
    target_reviews: int,
    *,
    candidate_appids=None,
    required_feasible: int | None = None,
    delay: float = 0.0,
    session=None,
    feasibility_probe=probe_review_feasibility,
    cache_path: Path | None = None,
    cache_snapshot_id: str | None = None,
) -> QualificationBatch:
    """
    Probe crawl feasibility for a deterministic subset of
    basically-qualified candidates.

    When required_feasible is set, probing stops as soon as
    that many feasible candidates have been found.

    Candidates not yet probed remain qualified/NEW and can
    be deferred to a later discovery cycle.
    """

    if target_reviews <= 0:
        raise ValueError(
            "target_reviews must be positive"
        )

    if (
        required_feasible is not None
        and required_feasible < 0
    ):
        raise ValueError(
            "required_feasible must be non-negative"
        )

    if (
        (cache_path is None)
        != (cache_snapshot_id is None)
    ):
        raise ValueError(
            "cache_path and cache_snapshot_id "
            "must be provided together"
        )

    cache_results: dict[int, dict] = {}

    if cache_path is not None:
        cache_results = (
            load_feasibility_cache(
                cache_path,
                snapshot_id=cache_snapshot_id,
                target_reviews=target_reviews,
            )
        )

    candidate_by_appid = {
        int(candidate["appid"]): dict(candidate)
        for candidate in qualification.qualified_candidates
    }

    if candidate_appids is None:
        probe_order = list(
            candidate_by_appid
        )
    else:
        probe_order = [
            int(appid)
            for appid in candidate_appids
        ]

    own_session = session is None

    if session is None:
        session = requests.Session()

    feasible_updates = {}
    crawl_rejected = {}
    feasible_found = 0

    try:
        for appid in probe_order:
            if (
                required_feasible is not None
                and feasible_found >= required_feasible
            ):
                break

            candidate = candidate_by_appid.get(
                appid
            )

            if candidate is None:
                raise RuntimeError(
                    "Feasibility probe appid is not "
                    "present in qualified candidates: "
                    f"{appid}"
                )

            cached = cache_results.get(
                appid
            )

            if cached is not None:
                result_payload = dict(
                    cached
                )
                source = "CACHE"

            else:
                result = feasibility_probe(
                    session,
                    appid,
                    target_reviews,
                    delay=delay,
                )

                result_payload = (
                    result.to_dict()
                )
                source = "LIVE"

                if cache_path is not None:
                    cache_results[
                        appid
                    ] = result_payload

                    save_feasibility_cache(
                        cache_path,
                        snapshot_id=(
                            cache_snapshot_id
                        ),
                        target_reviews=(
                            target_reviews
                        ),
                        results=(
                            cache_results
                        ),
                    )

            try:
                feasible = result_payload[
                    "feasible"
                ]

                unique_reviews = int(
                    result_payload[
                        "unique_reviews"
                    ]
                )

                status = str(
                    result_payload[
                        "status"
                    ]
                )

                if not isinstance(
                    feasible,
                    bool,
                ):
                    raise TypeError(
                        "feasible must be bool"
                    )

            except (
                KeyError,
                TypeError,
                ValueError,
            ) as exc:
                raise ValueError(
                    "Invalid feasibility result "
                    f"for appid {appid}"
                ) from exc

            enriched = {
                **candidate,
                "crawl_feasibility":
                    result_payload,
            }

            if feasible:
                feasible_updates[
                    appid
                ] = enriched

                feasible_found += 1

                prefix = (
                    "CRAWL FEASIBLE"
                )

                if source == "CACHE":
                    prefix += " [CACHE]"

                print(
                    f"{prefix}:",
                    appid,
                    f"{unique_reviews}/"
                    f"{target_reviews}",
                )

            else:
                crawl_rejected[
                    appid
                ] = {
                    **enriched,
                    "crawl_rejection_reason":
                        "INSUFFICIENT_CRAWLABLE_REVIEWS",
                }

                prefix = (
                    "CRAWL REJECTED"
                )

                if source == "CACHE":
                    prefix += " [CACHE]"

                print(
                    f"{prefix}:",
                    appid,
                    f"{unique_reviews}/"
                    f"{target_reviews}",
                    status,
                )

    finally:
        if own_session:
            session.close()

    qualified = []

    for candidate in (
        qualification.qualified_candidates
    ):
        appid = int(
            candidate["appid"]
        )

        if appid in crawl_rejected:
            continue

        qualified.append(
            feasible_updates.get(
                appid,
                dict(candidate),
            )
        )

    rejected = list(
        qualification.rejected_candidates
    )

    for appid in probe_order:
        candidate = crawl_rejected.get(
            appid
        )

        if candidate is not None:
            rejected.append(
                candidate
            )

    reason_counts = dict(
        qualification.rejection_reason_counts
    )

    if crawl_rejected:
        reason_counts[
            "INSUFFICIENT_CRAWLABLE_REVIEWS"
        ] = (
            reason_counts.get(
                "INSUFFICIENT_CRAWLABLE_REVIEWS",
                0,
            )
            + len(crawl_rejected)
        )

    return QualificationBatch(
        candidate_count=(
            qualification.candidate_count
        ),
        duplicate_candidate_count=(
            qualification
            .duplicate_candidate_count
        ),
        qualified_candidates=tuple(
            qualified
        ),
        rejected_candidates=tuple(
            rejected
        ),
        failed_candidates=(
            qualification.failed_candidates
        ),
        rejection_reason_counts=dict(
            sorted(
                reason_counts.items()
            )
        ),
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
    max_active_games: int = 100,
    feasibility_probe=None,
    feasibility_delay: float = 0.0,
    feasibility_cache_path: Path | None = None,
    feasibility_snapshot_id: str | None = None,
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

    capacity = calculate_onboarding_capacity(
        reconciliation.registry,
        max_active_games=max_active_games,
        max_new_games_per_cycle=(
            policy.onboarding
            .max_new_games_per_cycle
        ),
    )

    if (
        feasibility_probe is not None
        and capacity.new_queue_capacity > 0
        and reconciliation.pending_new_appids
    ):
        qualification = apply_crawl_feasibility(
            qualification,
            policy.onboarding.target_reviews_per_game,
            candidate_appids=(
                reconciliation.pending_new_appids
            ),
            required_feasible=(
                capacity.new_queue_capacity
            ),
            delay=feasibility_delay,
            feasibility_probe=feasibility_probe,
            cache_path=(
                feasibility_cache_path
            ),
            cache_snapshot_id=(
                feasibility_snapshot_id
            ),
        )

        reconciliation = reconcile_registry(
            qualification.qualified_candidates,
            reconciliation.registry,
            policy_version=policy.version,
            run_timestamp=run_timestamp,
        )

    planned_registry, plan = build_onboarding_plan(
        reconciliation,
        qualification.qualified_candidates,
        policy.onboarding,
        policy_version=policy.version,
        run_timestamp=run_timestamp,
        max_active_games=max_active_games,
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

def build_feasibility_snapshot_id(
    paths: Iterable[Path],
    *,
    snapshot_date: date | None = None,
) -> str:
    effective_date = (
        snapshot_date
        or datetime.now(timezone.utc).date()
    )

    iso_week = effective_date.isocalendar()

    snapshot_period = (
        f"{iso_week.year}-"
        f"W{iso_week.week:02d}"
    )

    content_fingerprint = (
        snapshot_fingerprint(
            list(paths)
        )
    )

    return (
        f"{snapshot_period}:"
        f"{content_fingerprint}"
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
        "--eligible-path", type=Path, default=ELIGIBLE_GAMES_PATH
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
        "--feasibility-cache-path",
        type=Path,
        default=DEFAULT_FEASIBILITY_CACHE_PATH,
    )
    parser.add_argument("--refresh-catalog", action="store_true")
    parser.add_argument("--refresh-page-size", type=int, default=None)
    parser.add_argument("--refresh-max-pages", type=int, default=None)
    parser.add_argument("--refresh-delay", type=float, default=None)
    parser.add_argument(
        "--max-active-games",
        type=int,
        default=int(os.getenv("STREAM_MAX_ACTIVE_GAMES", "100")),
    )
    parser.add_argument(
        "--as-of-date",
        type=date.fromisoformat,
        default=None,
        help="Optional YYYY-MM-DD date for reproducible release-age evaluation",
    )
    args = parser.parse_args()
    if args.max_active_games <= 0:
        parser.error("--max-active-games must be positive")

    policy = load_discovery_policy(args.policy_path)
    if args.refresh_catalog:
        refresh_result = refresh_catalog(
            policy=policy,
            policy_path=args.policy_path,
            registry_path=args.registry_path,
            candidates_path=args.candidates_path,
            metadata_path=args.metadata_path,
            eligible_path=args.eligible_path,
            review_probe_path=args.review_probe_path,
            page_size=args.refresh_page_size,
            max_pages=args.refresh_max_pages,
            delay=args.refresh_delay,
        )
        print(
            "Catalog refresh: "
            + json.dumps(refresh_result.to_dict(), ensure_ascii=False)
        )

    feasibility_snapshot_id = (
        build_feasibility_snapshot_id(
            [
                args.candidates_path,
                args.metadata_path,
                args.review_probe_path,
            ],
            snapshot_date=args.as_of_date,
        )
    )


    print(
        "Feasibility snapshot:",
        feasibility_snapshot_id,
    )

    candidate_evidence = build_candidate_evidence(
        read_jsonl(args.candidates_path),
        read_jsonl(args.metadata_path),
        read_jsonl(args.review_probe_path),
    )
    run_timestamp = datetime.now(timezone.utc).isoformat()
    existing_registry = load_registry(args.registry_path)
    existing_queue = _read_optional_jsonl(args.queue_path)
    result = run_control_plane(
    candidate_evidence,
    read_jsonl(args.snapshot_path),
    existing_registry,
    policy,
    current_date=args.as_of_date,
    run_timestamp=run_timestamp,
    max_active_games=args.max_active_games,
    feasibility_probe=probe_review_feasibility,
    feasibility_delay=(
        policy.catalog_refresh
        .request_delay_seconds
    ),
    feasibility_cache_path=(
        args.feasibility_cache_path
    ),
    feasibility_snapshot_id=(
        feasibility_snapshot_id
    ),
)

    reconciled_queue = reconcile_onboarding_queue(
        existing_queue,
        result.registry,
        policy.onboarding,
    )
    save_registry(args.registry_path, result.registry)
    _write_json(args.plan_path, result.onboarding_plan.to_dict())
    write_jsonl(args.queue_path, reconciled_queue)
    _write_json(args.report_path, result.run_report.to_dict())

    print(json.dumps(result.run_report.to_dict(), indent=2, ensure_ascii=False))
    print(f"Registry: {args.registry_path}")
    print(f"Onboarding plan: {args.plan_path}")
    print(f"Crawler queue: {args.queue_path}")
    print(f"Outstanding queued games: {len(reconciled_queue)}")
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


def _read_optional_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return read_jsonl(path)


if __name__ == "__main__":
    main()
