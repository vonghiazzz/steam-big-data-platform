"""Idempotently publish one validated onboarding batch to immutable HDFS Bronze."""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path

from src.onboarding.manifest import OnboardingManifest


HDFS_GAMES_ROOT = "/steam/bronze/games"
HDFS_REVIEWS_ROOT = "/steam/bronze/reviews"


@dataclass(frozen=True)
class PublishReport:
    batch_id: str
    game_target: str
    review_targets: tuple[str, ...]
    uploaded_targets: tuple[str, ...]
    reused_targets: tuple[str, ...]

    def to_dict(self) -> dict:
        return asdict(self)


def publish_hdfs_batch(
    manifest: OnboardingManifest,
    batch_path: Path,
    *,
    namenode_container: str,
) -> PublishReport:
    """Publish validated stage files without replacing an existing Bronze object."""
    games_source = batch_path / "games" / "games_raw.jsonl"
    reviews_source = batch_path / "reviews_by_game"
    if not games_source.is_file():
        raise RuntimeError(f"Missing staged games file: {games_source}")
    for appid in manifest.appids:
        path = reviews_source / f"{appid}.jsonl"
        if not path.is_file():
            raise RuntimeError(f"Missing staged review file: {path}")

    _require_running_container(namenode_container)
    stage_root = f"/tmp/steam_onboarding/{manifest.batch_id}"
    _container_run(namenode_container, "mkdir", "-p", f"{stage_root}/reviews", root=True)
    try:
        _docker_cp(games_source, namenode_container, f"{stage_root}/games.jsonl")
        for appid in manifest.appids:
            _docker_cp(
                reviews_source / f"{appid}.jsonl",
                namenode_container,
                f"{stage_root}/reviews/{appid}.jsonl",
            )
        _container_run(namenode_container, "chmod", "-R", "a+rX", stage_root, root=True)
        _hdfs(namenode_container, "-mkdir", "-p", HDFS_GAMES_ROOT, HDFS_REVIEWS_ROOT)

        game_target = f"{HDFS_GAMES_ROOT}/onboarding-{manifest.batch_id}.jsonl"
        uploaded: list[str] = []
        reused: list[str] = []
        _publish_one(
            games_source,
            f"{stage_root}/games.jsonl",
            game_target,
            namenode_container,
            uploaded,
            reused,
        )

        review_targets: list[str] = []
        for appid in manifest.appids:
            target = f"{HDFS_REVIEWS_ROOT}/{appid}.jsonl"
            review_targets.append(target)
            _publish_one(
                reviews_source / f"{appid}.jsonl",
                f"{stage_root}/reviews/{appid}.jsonl",
                target,
                namenode_container,
                uploaded,
                reused,
            )
    finally:
        _container_run(
            namenode_container,
            "rm",
            "-rf",
            stage_root,
            root=True,
            check=False,
        )

    report = PublishReport(
        batch_id=manifest.batch_id,
        game_target=game_target,
        review_targets=tuple(review_targets),
        uploaded_targets=tuple(uploaded),
        reused_targets=tuple(reused),
    )
    _write_json(batch_path / "publish_report.json", report.to_dict())
    return report


def _publish_one(
    local_path: Path,
    container_path: str,
    hdfs_target: str,
    container: str,
    uploaded: list[str],
    reused: list[str],
) -> None:
    exists = _hdfs(container, "-test", "-e", hdfs_target, check=False)
    if exists.returncode == 0:
        remote = _hdfs(container, "-cat", hdfs_target, text=False).stdout
        local = local_path.read_bytes()
        if hashlib.sha256(remote).digest() != hashlib.sha256(local).digest():
            raise RuntimeError(
                f"Refusing to overwrite non-identical Bronze object: {hdfs_target}"
            )
        reused.append(hdfs_target)
        return
    if exists.returncode != 1:
        raise RuntimeError(_command_error(exists, f"Could not inspect {hdfs_target}"))

    temporary = f"{hdfs_target}.tmp-{local_path.stem}"
    _hdfs(container, "-rm", "-f", temporary, check=False)
    _hdfs(container, "-put", container_path, temporary)
    try:
        _hdfs(container, "-mv", temporary, hdfs_target)
    except Exception:
        _hdfs(container, "-rm", "-f", temporary, check=False)
        raise
    uploaded.append(hdfs_target)


def _require_running_container(container: str) -> None:
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", container],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or result.stdout.strip() != "true":
        raise RuntimeError(f"NameNode container is not running: {container}")


def _docker_cp(source: Path, container: str, destination: str) -> None:
    result = subprocess.run(
        ["docker", "cp", str(source), f"{container}:{destination}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(_command_error(result, f"docker cp failed for {source}"))


def _container_run(
    container: str,
    *command: str,
    root: bool = False,
    check: bool = True,
) -> subprocess.CompletedProcess:
    args = ["docker", "exec"]
    if root:
        args.extend(["-u", "root"])
    args.extend([container, *command])
    result = subprocess.run(args, capture_output=True, text=True, check=False)
    if check and result.returncode != 0:
        raise RuntimeError(_command_error(result, "Container command failed"))
    return result


def _hdfs(
    container: str,
    *arguments: str,
    check: bool = True,
    text: bool = True,
) -> subprocess.CompletedProcess:
    result = subprocess.run(
        ["docker", "exec", container, "hdfs", "dfs", *arguments],
        capture_output=True,
        text=text,
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(_command_error(result, "HDFS command failed"))
    return result


def _command_error(result: subprocess.CompletedProcess, prefix: str) -> str:
    stderr = result.stderr
    stdout = result.stdout
    if isinstance(stderr, bytes):
        stderr = stderr.decode("utf-8", errors="replace")
    if isinstance(stdout, bytes):
        stdout = stdout.decode("utf-8", errors="replace")
    detail = (stderr or stdout or "unknown error").strip()
    return f"{prefix}: {detail}"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f"{path.name}.tmp"
    temporary.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
