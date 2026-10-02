from __future__ import annotations

import argparse
import json
import os
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from src.common.jsonl import read_jsonl, write_jsonl
from src.discovery.paths import (
    DISCOVERY_SCHEDULER_STATE_PATH,
    ONBOARDING_QUEUE_PATH,
    REGISTRY_PATH,
)
from src.discovery.policy import (
    DEFAULT_POLICY_PATH,
    load_discovery_policy,
)
from src.discovery.registry import (
    STATUS_NEW,
    STATUS_QUEUED,
    load_registry,
    reconcile_onboarding_queue,
    save_registry,
)
from src.discovery.scheduler import (
    load_scheduler_state,
    save_scheduler_state,
)


DEFAULT_STAGING_ROOT = Path("data/onboarding")

SAFE_STEPS = {
    "runtime",
    "prepare",
    "crawl",
}


def _utc_now() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RuntimeError(
            f"Required file does not exist: {path}"
        )

    payload = json.loads(
        path.read_text(
            encoding="utf-8"
        )
    )

    if not isinstance(payload, dict):
        raise RuntimeError(
            f"Expected JSON object: {path}"
        )

    return payload


def _write_json_atomic(
    path: Path,
    payload: Mapping[str, Any],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = (
        path.parent
        / f".{path.name}.{os.getpid()}.tmp"
    )

    temporary.write_text(
        json.dumps(
            payload,
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    os.replace(
        temporary,
        path,
    )


def recover_failed_batch(
    batch_id: str,
    *,
    staging_root: Path,
    registry_path: Path,
    queue_path: Path,
    scheduler_state_path: Path,
    policy_path: Path,
    reason: str,
) -> dict[str, Any]:
    batch_root = (
        staging_root
        / batch_id
    )

    state_path = (
        batch_root
        / "workflow_state.json"
    )

    manifest_path = (
        batch_root
        / "manifest.json"
    )

    state = _load_json(
        state_path
    )

    manifest = _load_json(
        manifest_path
    )

    # --------------------------------------------------
    # Workflow safety checks
    # --------------------------------------------------

    if state.get("batch_id") != batch_id:
        raise RuntimeError(
            "Workflow batch_id does not match "
            f"requested batch: {batch_id}"
        )

    if manifest.get("batch_id") != batch_id:
        raise RuntimeError(
            "Manifest batch_id does not match "
            f"requested batch: {batch_id}"
        )

    if state.get("status") != "FAILED":
        raise RuntimeError(
            "Recovery is allowed only for "
            "FAILED workflows; "
            f"found {state.get('status')!r}"
        )

    if state.get("current_phase") != "crawl":
        raise RuntimeError(
            "Recovery is allowed only when "
            "failure occurred in crawl; "
            f"found {state.get('current_phase')!r}"
        )

    steps = state.get(
        "steps",
        {},
    )

    if not isinstance(
        steps,
        Mapping,
    ):
        raise RuntimeError(
            "Workflow steps must be an object"
        )

    unexpected_steps = (
        set(steps)
        - SAFE_STEPS
    )

    if unexpected_steps:
        raise RuntimeError(
            "Refusing recovery because downstream "
            "workflow steps exist: "
            + ", ".join(
                sorted(
                    unexpected_steps
                )
            )
        )

    crawl_state = steps.get(
        "crawl",
        {},
    )

    if (
        not isinstance(
            crawl_state,
            Mapping,
        )
        or crawl_state.get("status")
        != "FAILED"
    ):
        raise RuntimeError(
            "Crawl step is not FAILED"
        )

    # --------------------------------------------------
    # Manifest
    # --------------------------------------------------

    games = manifest.get(
        "games"
    )

    if not isinstance(
        games,
        list,
    ) or not games:
        raise RuntimeError(
            "Manifest has no games"
        )

    manifest_appids = {
        int(game["appid"])
        for game in games
    }

    if len(manifest_appids) != len(games):
        raise RuntimeError(
            "Manifest contains duplicate appids"
        )

    # --------------------------------------------------
    # Registry
    # --------------------------------------------------

    registry = load_registry(
        registry_path
    )

    for appid in sorted(
        manifest_appids
    ):
        entry = registry.get(
            appid
        )

        if entry is None:
            raise RuntimeError(
                f"Manifest appid {appid} "
                "is missing from registry"
            )

        if entry.status != STATUS_QUEUED:
            raise RuntimeError(
                f"Manifest appid {appid} "
                "must be QUEUED; "
                f"found {entry.status}"
            )

    # --------------------------------------------------
    # Queue
    # --------------------------------------------------

    queue_rows = (
        read_jsonl(
            queue_path
        )
        if queue_path.exists()
        else []
    )

    queue_appids = {
        int(row["appid"])
        for row in queue_rows
    }

    if queue_appids != manifest_appids:
        raise RuntimeError(
            "Current onboarding queue does not "
            "exactly match failed batch manifest: "
            f"queue={sorted(queue_appids)}, "
            f"manifest={sorted(manifest_appids)}"
        )

    # --------------------------------------------------
    # Scheduler
    # --------------------------------------------------

    scheduler_state = (
        load_scheduler_state(
            scheduler_state_path
        )
    )

    if (
        scheduler_state.get(
            "active_batch_id"
        )
        != batch_id
    ):
        raise RuntimeError(
            "Scheduler active_batch_id does not "
            "match recovery batch"
        )

    # --------------------------------------------------
    # All preflight checks passed.
    #
    # Release failed batch games back to NEW.
    # --------------------------------------------------

    recovered_registry = dict(
        registry
    )

    for appid in manifest_appids:
        recovered_registry[appid] = replace(
            registry[appid],
            status=STATUS_NEW,
            queued_at=None,
        )

    policy = load_discovery_policy(
        policy_path
    )

    reconciled_queue = (
        reconcile_onboarding_queue(
            queue_rows,
            recovered_registry,
            policy.onboarding,
        )
    )

    if any(
        int(row["appid"])
        in manifest_appids
        for row in reconciled_queue
    ):
        raise RuntimeError(
            "Recovered batch games remained "
            "in onboarding queue"
        )

    # --------------------------------------------------
    # Persist registry + queue first.
    # Workflow becomes terminal only after release.
    # --------------------------------------------------

    save_registry(
        registry_path,
        recovered_registry,
    )

    write_jsonl(
        queue_path,
        reconciled_queue,
    )

    recovered_at = _utc_now()

    state.update(
        {
            "status": "ABANDONED",
            "updated_at": recovered_at,
            "abandoned_at": recovered_at,
            "abandon_reason": reason,
        }
    )

    _write_json_atomic(
        state_path,
        state,
    )

    scheduler_state[
        "active_batch_id"
    ] = None

    scheduler_state[
        "last_recovered_batch_id"
    ] = batch_id

    scheduler_state[
        "last_recovery_at"
    ] = recovered_at

    save_scheduler_state(
        scheduler_state_path,
        scheduler_state,
    )

    return {
        "batch_id": batch_id,
        "status": "ABANDONED",
        "released_appids": sorted(
            manifest_appids
        ),
        "released_count": len(
            manifest_appids
        ),
        "remaining_queue_count": len(
            reconciled_queue
        ),
        "recovered_at": recovered_at,
    }


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--batch-id",
        required=True,
    )

    parser.add_argument(
        "--staging-root",
        type=Path,
        default=DEFAULT_STAGING_ROOT,
    )

    parser.add_argument(
        "--registry",
        type=Path,
        default=REGISTRY_PATH,
    )

    parser.add_argument(
        "--queue",
        type=Path,
        default=ONBOARDING_QUEUE_PATH,
    )

    parser.add_argument(
        "--scheduler-state",
        type=Path,
        default=DISCOVERY_SCHEDULER_STATE_PATH,
    )

    parser.add_argument(
        "--policy",
        type=Path,
        default=DEFAULT_POLICY_PATH,
    )

    parser.add_argument(
        "--reason",
        default=(
            "Historical crawl cannot satisfy "
            "target review count; batch replaced "
            "after crawl-feasibility gate fix"
        ),
    )

    args = parser.parse_args()

    result = recover_failed_batch(
        args.batch_id,
        staging_root=args.staging_root,
        registry_path=args.registry,
        queue_path=args.queue,
        scheduler_state_path=(
            args.scheduler_state
        ),
        policy_path=args.policy,
        reason=args.reason,
    )

    print(
        json.dumps(
            result,
            indent=2,
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()