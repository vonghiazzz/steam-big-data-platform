import json

from common.config import (
    BRONZE_READY_FINALIZATION_REPORT_PATH,
    BRONZE_READY_REVIEWS_ROOT,
    LANDING_REVIEWS_ROOT,
    SELECTED_GAMES_PATH,
    TARGET_GAME_COUNT,
    TARGET_REVIEWS_PER_GAME,
)
from common.jsonl import iter_jsonl


def main():
    selected_appids = [
        int(row["appid"])
        for row in iter_jsonl(
            SELECTED_GAMES_PATH
        )
    ]

    if (
        len(selected_appids)
        != TARGET_GAME_COUNT
    ):
        raise RuntimeError(
            f"Expected "
            f"{TARGET_GAME_COUNT} "
            f"selected games, found "
            f"{len(selected_appids)}."
        )

    if (
        len(set(selected_appids))
        != TARGET_GAME_COUNT
    ):
        raise RuntimeError(
            "Selected game manifest "
            "contains duplicate AppIDs."
        )

    BRONZE_READY_REVIEWS_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    game_reports = []

    total_input = 0
    total_output = 0
    total_dropped = 0
    total_duplicates = 0

    failures = []

    for appid in selected_appids:
        source_path = (
            LANDING_REVIEWS_ROOT
            / f"{appid}.jsonl"
        )

        output_path = (
            BRONZE_READY_REVIEWS_ROOT
            / f"{appid}.jsonl"
        )

        if not source_path.exists():
            failures.append(
                f"{appid}: "
                "source review file missing"
            )
            continue

        seen_ids = set()

        kept_rows = []

        input_count = 0
        duplicate_count = 0

        for row in iter_jsonl(
            source_path
        ):
            input_count += 1

            review = (
                row.get("review")
                or {}
            )

            recommendation_id = (
                review.get(
                    "recommendationid"
                )
            )

            if recommendation_id is None:
                raise RuntimeError(
                    f"{source_path}: "
                    "missing recommendationid"
                )

            recommendation_id = str(
                recommendation_id
            )

            if (
                recommendation_id
                in seen_ids
            ):
                duplicate_count += 1
                continue

            seen_ids.add(
                recommendation_id
            )

            if (
                len(kept_rows)
                < TARGET_REVIEWS_PER_GAME
            ):
                kept_rows.append(
                    row
                )

        output_count = len(
            kept_rows
        )

        if (
            output_count
            != TARGET_REVIEWS_PER_GAME
        ):
            failures.append(
                f"{appid}: expected "
                f"{TARGET_REVIEWS_PER_GAME} "
                f"unique reviews, found "
                f"{output_count}"
            )

        temp_path = (
            output_path.parent
            / f"{output_path.name}.tmp"
        )

        with temp_path.open(
            "w",
            encoding="utf-8",
        ) as file:
            for row in kept_rows:
                file.write(
                    json.dumps(
                        row,
                        ensure_ascii=False,
                    )
                )
                file.write("\n")

        temp_path.replace(
            output_path
        )

        dropped_count = (
            input_count
            - output_count
        )

        total_input += (
            input_count
        )

        total_output += (
            output_count
        )

        total_dropped += (
            dropped_count
        )

        total_duplicates += (
            duplicate_count
        )

        game_reports.append(
            {
                "appid": appid,
                "input_records": (
                    input_count
                ),
                "output_records": (
                    output_count
                ),
                "dropped_records": (
                    dropped_count
                ),
                "duplicates_seen": (
                    duplicate_count
                ),
            }
        )

    result = (
        "PASS"
        if not failures
        else "FAIL"
    )

    report = {
        "result": result,
        "target_reviews_per_game": (
            TARGET_REVIEWS_PER_GAME
        ),
        "selected_games": (
            len(selected_appids)
        ),
        "total_input_records": (
            total_input
        ),
        "total_output_records": (
            total_output
        ),
        "total_dropped_records": (
            total_dropped
        ),
        "duplicates_seen": (
            total_duplicates
        ),
        "failures": failures,
        "games": game_reports,
    }

    (
        BRONZE_READY_FINALIZATION_REPORT_PATH
        .parent
        .mkdir(
            parents=True,
            exist_ok=True,
        )
    )

    (
        BRONZE_READY_FINALIZATION_REPORT_PATH
        .write_text(
            json.dumps(
                report,
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
    )

    print("=" * 80)
    print(
        "STEAM REVIEW SAMPLE FINALIZATION"
    )
    print("=" * 80)

    print(
        "Selected games        :",
        len(selected_appids),
    )

    print(
        "Target / game         :",
        TARGET_REVIEWS_PER_GAME,
    )

    print(
        "Input records         :",
        total_input,
    )

    print(
        "Final records         :",
        total_output,
    )

    print(
        "Dropped overshoot     :",
        total_dropped,
    )

    print(
        "Duplicates encountered:",
        total_duplicates,
    )

    print()

    print(
        "RESULT                :",
        result,
    )

    if failures:
        print()
        print("FAILURES:")

        for failure in failures:
            print(
                "-",
                failure,
            )

    print()
    print("Output:")
    print(
        BRONZE_READY_REVIEWS_ROOT
    )

    print()
    print("Report:")
    print(
        BRONZE_READY_FINALIZATION_REPORT_PATH
    )


if __name__ == "__main__":
    main()
