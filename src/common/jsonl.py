import json
from pathlib import Path
from typing import Any, Iterable, Iterator


def iter_jsonl(
    path: Path,
) -> Iterator[dict[str, Any]]:
    with path.open(
        "r",
        encoding="utf-8",
    ) as file:
        for line_number, line in enumerate(
            file,
            start=1,
        ):
            line = line.strip()

            if not line:
                continue

            try:
                row = json.loads(
                    line
                )
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"Invalid JSON in "
                    f"{path} "
                    f"at line "
                    f"{line_number}: "
                    f"{exc}"
                ) from exc

            if not isinstance(
                row,
                dict,
            ):
                raise RuntimeError(
                    f"Expected JSON object "
                    f"in {path} "
                    f"at line "
                    f"{line_number}."
                )

            yield row


def read_jsonl(
    path: Path,
) -> list[dict[str, Any]]:
    return list(
        iter_jsonl(path)
    )


def write_jsonl(
    path: Path,
    rows: Iterable[
        dict[str, Any]
    ],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = (
        path.parent
        / f"{path.name}.tmp"
    )

    with temp_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        for row in rows:
            file.write(
                json.dumps(
                    row,
                    ensure_ascii=False,
                )
            )

            file.write("\n")

    temp_path.replace(
        path
    )


def append_jsonl(
    path: Path,
    row: dict[str, Any],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "a",
        encoding="utf-8",
    ) as file:
        file.write(
            json.dumps(
                row,
                ensure_ascii=False,
            )
        )

        file.write("\n")


def count_jsonl(
    path: Path,
) -> int:
    return sum(
        1
        for _ in iter_jsonl(path)
    )
