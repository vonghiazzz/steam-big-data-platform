import argparse
import json
from pathlib import Path

from common.config import (
    LANDING_GAMES_RAW_PATH,
    METADATA_PROBE_RAW_PATH,
    SELECTED_GAMES_PATH,
    TARGET_GAME_COUNT,
)
from common.jsonl import iter_jsonl


def load_selected_appids() -> set[int]:
    selected_appids = {
        int(row["appid"])
        for row in iter_jsonl(
            SELECTED_GAMES_PATH
        )
    }

    if (
        len(selected_appids)
        != TARGET_GAME_COUNT
    ):
        raise RuntimeError(
            f"Expected "
            f"{TARGET_GAME_COUNT} "
            f"selected AppIDs, found "
            f"{len(selected_appids)}."
        )

    return selected_appids


def prepare_raw_metadata(
    output_path: Path,
) -> None:
    selected_appids = (
        load_selected_appids()
    )

    matched_appids = set()
    matched_lines = []

    with METADATA_PROBE_RAW_PATH.open(
        "r",
        encoding="utf-8",
    ) as source:
        for line_number, raw_line in enumerate(
            source,
            start=1,
        ):
            stripped = raw_line.strip()

            if not stripped:
                continue

            try:
                row = json.loads(
                    stripped
                )
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"Invalid JSON in "
                    f"{METADATA_PROBE_RAW_PATH} "
                    f"at line {line_number}: "
                    f"{exc}"
                ) from exc

            appid = int(
                row["appid"]
            )

            if (
                appid
                not in selected_appids
            ):
                continue

            if appid in matched_appids:
                raise RuntimeError(
                    f"Duplicate raw metadata "
                    f"record for AppID "
                    f"{appid}."
                )

            matched_appids.add(
                appid
            )

            if raw_line.endswith(
                "\n"
            ):
                matched_lines.append(
                    raw_line
                )
            else:
                matched_lines.append(
                    raw_line + "\n"
                )

    missing_appids = (
        selected_appids
        - matched_appids
    )

    extra_appids = (
        matched_appids
        - selected_appids
    )

    if missing_appids:
        raise RuntimeError(
            "Missing raw metadata for "
            f"{len(missing_appids)} "
            "selected games: "
            f"{sorted(missing_appids)}"
        )

    if extra_appids:
        raise RuntimeError(
            "Unexpected metadata for "
            f"{len(extra_appids)} "
            "games."
        )

    if (
        len(matched_lines)
        != TARGET_GAME_COUNT
    ):
        raise RuntimeError(
            f"Expected "
            f"{TARGET_GAME_COUNT} "
            f"output records, found "
            f"{len(matched_lines)}."
        )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = (
        output_path.parent
        / f"{output_path.name}.tmp"
    )

    temp_path.write_text(
        "".join(
            matched_lines
        ),
        encoding="utf-8",
    )

    temp_path.replace(
        output_path
    )

    print("=" * 80)
    print(
        "STEAM RAW GAME METADATA PREPARATION"
    )
    print("=" * 80)

    print(
        "Selected AppIDs       :",
        len(selected_appids),
    )

    print(
        "Matched raw records   :",
        len(matched_lines),
    )

    print(
        "Unique output AppIDs  :",
        len(matched_appids),
    )

    print(
        "Selection match       :",
        matched_appids
        == selected_appids,
    )

    print()
    print("Output:")
    print(
        output_path
    )


def main():
    parser = (
        argparse.ArgumentParser()
    )

    parser.add_argument(
        "--output-path",
        type=Path,
        default=(
            LANDING_GAMES_RAW_PATH
        ),
    )

    args = (
        parser.parse_args()
    )

    prepare_raw_metadata(
        output_path=(
            args.output_path
        )
    )


if __name__ == "__main__":
    main()
