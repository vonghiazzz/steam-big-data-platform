from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Iterable


CACHE_SCHEMA_VERSION = 1


def snapshot_fingerprint(
    paths: Iterable[Path],
) -> str:
    digest = hashlib.sha256()

    for path in paths:
        path = Path(path)

        digest.update(
            str(path.name).encode("utf-8")
        )
        digest.update(b"\0")

        with path.open("rb") as file:
            while True:
                chunk = file.read(
                    1024 * 1024
                )

                if not chunk:
                    break

                digest.update(chunk)

        digest.update(b"\0")

    return digest.hexdigest()


def load_feasibility_cache(
    path: Path,
    *,
    snapshot_id: str,
    target_reviews: int,
) -> dict[int, dict]:
    if not path.exists():
        return {}

    try:
        payload = json.loads(
            path.read_text(
                encoding="utf-8"
            )
        )
    except (
        json.JSONDecodeError,
        OSError,
    ):
        return {}

    if (
        payload.get("schema_version")
        != CACHE_SCHEMA_VERSION
    ):
        return {}

    if (
        payload.get("snapshot_id")
        != snapshot_id
    ):
        return {}

    if (
        payload.get("target_reviews")
        != target_reviews
    ):
        return {}

    results = payload.get(
        "results",
        {}
    )

    if not isinstance(
        results,
        dict,
    ):
        return {}

    cache = {}

    for raw_appid, result in (
        results.items()
    ):
        try:
            appid = int(raw_appid)
        except (
            TypeError,
            ValueError,
        ):
            continue

        if not isinstance(
            result,
            dict,
        ):
            continue

        cache[appid] = result

    return cache


def save_feasibility_cache(
    path: Path,
    *,
    snapshot_id: str,
    target_reviews: int,
    results: dict[int, dict],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    payload = {
        "schema_version":
            CACHE_SCHEMA_VERSION,
        "snapshot_id":
            snapshot_id,
        "target_reviews":
            target_reviews,
        "results": {
            str(appid): result
            for appid, result
            in sorted(
                results.items()
            )
        },
    }

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