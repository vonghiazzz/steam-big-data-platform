"""Read-only HDFS collision checks for onboarding dry-runs."""

from __future__ import annotations

import json
import subprocess
from dataclasses import asdict, dataclass

from src.onboarding.manifest import OnboardingManifest


@dataclass(frozen=True)
class HdfsPreflightReport:
    namenode_container: str
    existing_game_appids: tuple[int, ...]
    game_collisions: tuple[int, ...]
    review_collisions: tuple[int, ...]
    errors: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.errors and not self.game_collisions and not self.review_collisions

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["passed"] = self.passed
        return payload


def inspect_hdfs_collisions(
    manifest: OnboardingManifest,
    *,
    namenode_container: str,
    games_glob: str = "/steam/bronze/games/*.jsonl",
    reviews_root: str = "/steam/bronze/reviews",
) -> HdfsPreflightReport:
    """Read existing Bronze keys; never create, delete, or overwrite paths."""
    errors: list[str] = []
    existing_games: set[int] = set()

    result = _docker_exec(
        namenode_container,
        "hdfs",
        "dfs",
        "-cat",
        games_glob,
    )
    if result.returncode != 0:
        errors.append(
            "Could not read HDFS Bronze games: "
            + (result.stderr.strip() or result.stdout.strip())
        )
    else:
        for line_number, line in enumerate(result.stdout.split("\n"), start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                existing_games.add(int(row["appid"]))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                errors.append(
                    f"HDFS Bronze games contains invalid row at line {line_number}"
                )
                break

    review_collisions: list[int] = []
    for appid in manifest.appids:
        test = _docker_exec(
            namenode_container,
            "hdfs",
            "dfs",
            "-test",
            "-e",
            f"{reviews_root.rstrip('/')}/{appid}.jsonl",
        )
        if test.returncode == 0:
            review_collisions.append(appid)
        elif test.returncode != 1:
            errors.append(
                f"Could not test HDFS review path for appid {appid}: "
                + (test.stderr.strip() or test.stdout.strip())
            )

    game_collisions = sorted(set(manifest.appids) & existing_games)
    return HdfsPreflightReport(
        namenode_container=namenode_container,
        existing_game_appids=tuple(sorted(existing_games)),
        game_collisions=tuple(game_collisions),
        review_collisions=tuple(sorted(review_collisions)),
        errors=tuple(errors),
    )


def _docker_exec(container: str, *command: str) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["docker", "exec", container, *command],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        return subprocess.CompletedProcess(
            args=["docker", "exec", container, *command],
            returncode=127,
            stdout="",
            stderr=str(exc),
        )
