"""Prepare isolated local files for one onboarding batch."""

from __future__ import annotations

from pathlib import Path

from src.common.jsonl import read_jsonl, write_jsonl
from src.onboarding.manifest import OnboardingManifest, save_manifest


def batch_root(staging_root: Path, batch_id: str) -> Path:
    return staging_root / batch_id


def prepare_batch_layout(
    staging_root: Path,
    manifest: OnboardingManifest,
    metadata_probe_path: Path,
) -> Path:
    """Create an idempotent staging layout and copy matching raw metadata."""
    root = batch_root(staging_root, manifest.batch_id)
    games_root = root / "games"
    reviews_root = root / "reviews_by_game"
    pages_root = root / "review_pages"
    games_root.mkdir(parents=True, exist_ok=True)
    reviews_root.mkdir(parents=True, exist_ok=True)
    pages_root.mkdir(parents=True, exist_ok=True)

    save_manifest(root / "manifest.json", manifest)
    stage_metadata_from_probe(
        manifest,
        metadata_probe_path,
        games_root / "games_raw.jsonl",
    )
    return root


def stage_metadata_from_probe(
    manifest: OnboardingManifest,
    metadata_probe_path: Path,
    output_path: Path,
) -> None:
    """Select exactly one successful raw metadata record per manifest game."""
    expected = set(manifest.appids)
    selected: dict[int, dict] = {}

    for row in read_jsonl(metadata_probe_path):
        try:
            appid = int(row.get("appid"))
        except (TypeError, ValueError):
            continue
        if appid not in expected:
            continue
        if appid in selected:
            raise ValueError(
                f"Metadata probe contains duplicate appid {appid}"
            )
        if row.get("success") is not True or not isinstance(row.get("data"), dict):
            raise ValueError(
                f"Metadata probe appid {appid} is not a successful raw record"
            )
        selected[appid] = row

    missing = sorted(expected - set(selected))
    if missing:
        raise ValueError(f"Metadata probe is missing manifest appids: {missing}")
    write_jsonl(output_path, (selected[appid] for appid in sorted(selected)))
