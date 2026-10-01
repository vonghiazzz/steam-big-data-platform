"""Plan, stage, and validate a dynamic Steam onboarding batch.

Phase A deliberately exposes no publish or activation command. HDFS writes and
registry transitions require later approval after the dry-run and tests pass.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.common.jsonl import read_jsonl
from src.discovery.policy import load_discovery_policy
from src.discovery.registry import load_registry
from src.onboarding.hdfs_preflight import inspect_hdfs_collisions
from src.onboarding.manifest import build_manifest, load_manifest
from src.onboarding.staging import batch_root, prepare_batch_layout
from src.onboarding.validation import (
    save_validation_report,
    validate_staged_batch,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_QUEUE = PROJECT_ROOT / "data/raw/registry/onboarding_queue.jsonl"
DEFAULT_REGISTRY = PROJECT_ROOT / "data/raw/registry/game_registry.jsonl"
DEFAULT_POLICY = PROJECT_ROOT / "config/discovery_policy.json"
DEFAULT_METADATA_PROBE = PROJECT_ROOT / "data/raw/catalog_probe/metadata_probe_raw.jsonl"
DEFAULT_BASELINE_REVIEWS = PROJECT_ROOT / "data/raw/bronze_ready/reviews_by_game"
DEFAULT_STAGING_ROOT = PROJECT_ROOT / "data/onboarding"
DEFAULT_CANONICAL_GAMES = PROJECT_ROOT / "data/raw/landing/games/games_raw.jsonl"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--queue", type=Path, default=DEFAULT_QUEUE)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--metadata-probe", type=Path, default=DEFAULT_METADATA_PROBE)
    parser.add_argument("--baseline-reviews", type=Path, default=DEFAULT_BASELINE_REVIEWS)
    parser.add_argument("--canonical-games", type=Path, default=DEFAULT_CANONICAL_GAMES)
    parser.add_argument("--staging-root", type=Path, default=DEFAULT_STAGING_ROOT)
    parser.add_argument(
        "--namenode-container",
        default="bda501-namenode",
    )
    parser.add_argument(
        "--skip-hdfs",
        action="store_true",
        help="skip read-only HDFS checks when developing offline",
    )
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--dry-run", action="store_true")
    action.add_argument("--prepare", action="store_true")
    action.add_argument("--validate", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.validate:
        return _validate(args)

    manifest = build_manifest(
        args.batch_id,
        read_jsonl(args.queue),
        load_registry(args.registry),
        load_discovery_policy(args.policy),
    )
    preflight = _local_preflight(
        manifest.appids,
        args.metadata_probe,
        args.baseline_reviews,
        args.canonical_games,
    )
    hdfs = None
    if not args.skip_hdfs:
        hdfs = inspect_hdfs_collisions(
            manifest,
            namenode_container=args.namenode_container,
        )

    passed = preflight["passed"] and (hdfs is None or hdfs.passed)
    summary = {
        "result": "PASS" if passed else "FAIL",
        "mode": "dry-run" if args.dry_run else "prepare",
        "manifest": manifest.to_dict(),
        "staging_path": str(batch_root(args.staging_root, manifest.batch_id)),
        "local_preflight": preflight,
        "hdfs_preflight": hdfs.to_dict() if hdfs else {"skipped": True},
        "writes_performed": False,
        "steam_api_called": False,
        "registry_changed": False,
    }

    if not passed:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return 1
    if args.prepare:
        root = prepare_batch_layout(
            args.staging_root,
            manifest,
            args.metadata_probe,
        )
        summary["writes_performed"] = True
        summary["prepared_path"] = str(root)
        summary["next_command"] = (
            "python -m src.ingestion.review_crawler "
            f"--games-path {args.queue} "
            f"--target-reviews {manifest.target_reviews_per_game} "
            f"--output-root {root}"
        )

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


def _validate(args: argparse.Namespace) -> int:
    root = batch_root(args.staging_root, args.batch_id)
    manifest = load_manifest(root / "manifest.json")
    report = validate_staged_batch(
        manifest,
        root,
        baseline_reviews_root=args.baseline_reviews,
    )
    save_validation_report(root / "validation_report.json", report)
    print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))
    return 0 if report.passed else 1


def _local_preflight(
    appids: tuple[int, ...],
    metadata_probe: Path,
    baseline_reviews: Path,
    canonical_games: Path,
) -> dict:
    expected = set(appids)
    errors: list[str] = []
    metadata_appids = _read_appids(metadata_probe, errors, "metadata probe")
    canonical_appids = _read_appids(canonical_games, errors, "canonical games")
    missing_metadata = sorted(expected - metadata_appids)
    game_collisions = sorted(expected & canonical_appids)
    review_collisions = sorted(
        appid
        for appid in expected
        if (baseline_reviews / f"{appid}.jsonl").exists()
    )
    if missing_metadata:
        errors.append(f"Metadata probe is missing appids: {missing_metadata}")
    if game_collisions:
        errors.append(f"Canonical games already contains appids: {game_collisions}")
    if review_collisions:
        errors.append(
            f"Canonical reviews already contains appids: {review_collisions}"
        )
    return {
        "passed": not errors,
        "metadata_probe_appids": len(metadata_appids),
        "canonical_game_appids": len(canonical_appids),
        "missing_metadata": missing_metadata,
        "game_collisions": game_collisions,
        "review_collisions": review_collisions,
        "errors": errors,
    }


def _read_appids(path: Path, errors: list[str], label: str) -> set[int]:
    if not path.exists():
        errors.append(f"Missing {label}: {path}")
        return set()
    appids: set[int] = set()
    try:
        for row in read_jsonl(path):
            try:
                appids.add(int(row["appid"]))
            except (KeyError, TypeError, ValueError):
                errors.append(f"{label} contains an invalid appid")
    except RuntimeError as exc:
        errors.append(str(exc))
    return appids


if __name__ == "__main__":
    raise SystemExit(main())
