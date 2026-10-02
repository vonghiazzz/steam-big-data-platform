from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from src.common.jsonl import read_jsonl
from src.discovery.paths import (
    DISCOVERY_SCHEDULER_STATE_PATH,
    ONBOARDING_QUEUE_PATH,
    REGISTRY_PATH,
)
from src.discovery.registry import (
    STATUS_NEW,
    load_registry,
)
from src.discovery.scheduler import (
    load_scheduler_state,
)


DEFAULT_STAGING_ROOT = Path("data/onboarding")

SAFE_STEPS = {
    "runtime",
    "prepare",
    "crawl",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RuntimeError(
            f"Required file does not exist: {path}"
        )

    payload = json.loads(
        path.read_text(encoding="utf-8")
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


def abandon_stale_failed_batch(
    batch_id: str,
    *,
    staging_root: Path,
    registry_path: Path,
    queue_path: Path,
    scheduler_state_path: Path,
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

    # ----------------------------------------
    # Workflow must be an early crawl failure.
    # ----------------------------------------

    if state.get("batch_id") != batch_id:
        raise RuntimeError(
            "Workflow batch_id mismatch"
        )

    if manifest.get("batch_id") != batch_id:
        raise RuntimeError(
            "Manifest batch_id mismatch"
        )

    if state.get("status") != "FAILED":
        raise RuntimeError(
            "Only FAILED batches may be "
            "abandoned; found "
            f"{state.get('status')!r}"
        )

    if state.get("current_phase") != "crawl":
        raise RuntimeError(
            "Only crawl-phase failures are "
            "allowed; found "
            f"{state.get('current_phase')!r}"
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

    unexpected = (
        set(steps)
        - SAFE_STEPS
    )

    if unexpected:
        raise RuntimeError(
            "Refusing abandonment because "
            "downstream steps exist: "
            + ", ".join(
                sorted(unexpected)
            )
        )

    crawl = steps.get(
        "crawl",
        {},
    )

    if (
        not isinstance(crawl, Mapping)
        or crawl.get("status") != "FAILED"
    ):
        raise RuntimeError(
            "Crawl step must be FAILED"
        )

    # ----------------------------------------
    # Manifest.
    # ----------------------------------------

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

    appids = {
        int(game["appid"])
        for game in games
    }

    if len(appids) != len(games):
        raise RuntimeError(
            "Manifest contains duplicate appids"
        )

    # ----------------------------------------
    # Registry must already be released to NEW.
    # ----------------------------------------

    registry = load_registry(
        registry_path
    )

    for appid in sorted(appids):
        entry = registry.get(
            appid
        )

        if entry is None:
            raise RuntimeError(
                f"appid {appid} missing "
                "from registry"
            )

        if entry.status != STATUS_NEW:
            raise RuntimeError(
                f"appid {appid} must already "
                f"be NEW; found {entry.status}"
            )

    # ----------------------------------------
    # Queue must no longer contain these games.
    # ----------------------------------------

    queue_rows = (
        read_jsonl(queue_path)
        if queue_path.exists()
        else []
    )

    queued_appids = {
        int(row["appid"])
        for row in queue_rows
    }

    overlap = (
        appids
        & queued_appids
    )

    if overlap:
        raise RuntimeError(
            "Refusing abandonment because "
            "manifest games remain queued: "
            + str(sorted(overlap))
        )

    # ----------------------------------------
    # Scheduler must not actively own batch.
    # ----------------------------------------

    scheduler_state = (
        load_scheduler_state(
            scheduler_state_path
        )
    )

    active_batch_id = (
        scheduler_state.get(
            "active_batch_id"
        )
    )

    if active_batch_id == batch_id:
        raise RuntimeError(
            "Scheduler still owns this batch"
        )

    if active_batch_id is not None:
        raise RuntimeError(
            "Scheduler currently owns another "
            f"batch: {active_batch_id}"
        )

    # ----------------------------------------
    # Safe to terminalize metadata only.
    # ----------------------------------------

    abandoned_at = _utc_now()

    state.update(
        {
            "status": "ABANDONED",
            "updated_at": abandoned_at,
            "abandoned_at": abandoned_at,
            "abandon_reason": reason,
            "abandon_mode":
                "METADATA_ONLY_ALREADY_RELEASED",
        }
    )

    _write_json_atomic(
        state_path,
        state,
    )

    return {
        "batch_id": batch_id,
        "status": "ABANDONED",
        "game_count": len(appids),
        "registry_mutated": False,
        "queue_mutated": False,
        "abandoned_at": abandoned_at,
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
        "--reason",
        default=(
            "Superseded crawl-only failed attempt; "
            "registry games were already released "
            "to NEW by guarded recovery"
        ),
    )

    args = parser.parse_args()

    result = abandon_stale_failed_batch(
        args.batch_id,
        staging_root=args.staging_root,
        registry_path=args.registry,
        queue_path=args.queue,
        scheduler_state_path=(
            args.scheduler_state
        ),
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