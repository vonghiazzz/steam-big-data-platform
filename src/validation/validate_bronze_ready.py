import json
from collections import Counter

from common.config import (
    BRONZE_READY_REVIEWS_ROOT,
    BRONZE_READY_VALIDATION_REPORT_PATH,
    SELECTED_GAMES_PATH,
    TARGET_GAME_COUNT,
    TARGET_REVIEWS_PER_GAME,
)
from common.jsonl import iter_jsonl


EXPECTED_TOTAL_REVIEWS = (
    TARGET_GAME_COUNT
    * TARGET_REVIEWS_PER_GAME
)


def main():
    selected_rows = list(
        iter_jsonl(
            SELECTED_GAMES_PATH
        )
    )

    selected_appids = [
        int(row["appid"])
        for row in selected_rows
    ]

    selected_set = set(
        selected_appids
    )

    review_files = sorted(
        BRONZE_READY_REVIEWS_ROOT.glob(
            "*.jsonl"
        )
    )

    file_appids = {
        int(path.stem)
        for path in review_files
    }

    errors = []
    warnings = []

    total_records = 0

    global_ids = set()

    duplicate_ids = 0

    language_counts = Counter()
    label_counts = Counter()

    empty_review_text = 0
    missing_recommendation_id = 0
    missing_voted_up = 0
    missing_appid = 0
    appid_mismatch = 0

    per_game_counts = {}

    if (
        len(selected_appids)
        != TARGET_GAME_COUNT
    ):
        errors.append(
            f"Expected "
            f"{TARGET_GAME_COUNT} "
            f"selected games, "
            f"found "
            f"{len(selected_appids)}."
        )

    if (
        len(selected_set)
        != len(selected_appids)
    ):
        errors.append(
            "Selected game manifest "
            "contains duplicate AppIDs."
        )

    missing_files = (
        selected_set
        - file_appids
    )

    extra_files = (
        file_appids
        - selected_set
    )

    if missing_files:
        errors.append(
            f"Missing review files for "
            f"{len(missing_files)} games."
        )

    if extra_files:
        errors.append(
            f"Found review files for "
            f"{len(extra_files)} "
            "unselected games."
        )

    for path in review_files:
        file_appid = int(
            path.stem
        )

        game_count = 0

        for row in iter_jsonl(path):
            total_records += 1
            game_count += 1

            row_appid = row.get(
                "appid"
            )

            if row_appid is None:
                missing_appid += 1

            elif int(row_appid) != file_appid:
                appid_mismatch += 1

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
                missing_recommendation_id += 1

            else:
                recommendation_id = str(
                    recommendation_id
                )

                if (
                    recommendation_id
                    in global_ids
                ):
                    duplicate_ids += 1

                global_ids.add(
                    recommendation_id
                )

            language = review.get(
                "language"
            )

            if language is None:
                language_counts[
                    "<missing>"
                ] += 1
            else:
                language_counts[
                    str(language)
                ] += 1

            voted_up = review.get(
                "voted_up"
            )

            if voted_up is True:
                label_counts[
                    "positive"
                ] += 1

            elif voted_up is False:
                label_counts[
                    "negative"
                ] += 1

            else:
                missing_voted_up += 1

            review_text = review.get(
                "review"
            )

            if (
                review_text is None
                or not str(
                    review_text
                ).strip()
            ):
                empty_review_text += 1

        per_game_counts[
            file_appid
        ] = game_count

        if (
            game_count
            != TARGET_REVIEWS_PER_GAME
        ):
            errors.append(
                f"{file_appid}: expected "
                f"{TARGET_REVIEWS_PER_GAME} "
                f"reviews, found "
                f"{game_count}."
            )

    if (
        total_records
        != EXPECTED_TOTAL_REVIEWS
    ):
        errors.append(
            f"Expected "
            f"{EXPECTED_TOTAL_REVIEWS} "
            f"total reviews, found "
            f"{total_records}."
        )

    if (
        len(global_ids)
        != EXPECTED_TOTAL_REVIEWS
    ):
        errors.append(
            f"Expected "
            f"{EXPECTED_TOTAL_REVIEWS} "
            "unique recommendation IDs, "
            f"found {len(global_ids)}."
        )

    if duplicate_ids > 0:
        errors.append(
            f"Found {duplicate_ids} "
            "duplicate recommendation IDs."
        )

    non_english = sum(
        count
        for language, count
        in language_counts.items()
        if language != "english"
    )

    if non_english > 0:
        errors.append(
            f"Found {non_english} "
            "non-English records."
        )

    if (
        missing_recommendation_id
        > 0
    ):
        errors.append(
            f"Found "
            f"{missing_recommendation_id} "
            "records without "
            "recommendationid."
        )

    if missing_voted_up > 0:
        errors.append(
            f"Found "
            f"{missing_voted_up} "
            "records without valid "
            "voted_up."
        )

    if missing_appid > 0:
        errors.append(
            f"Found {missing_appid} "
            "records without appid."
        )

    if appid_mismatch > 0:
        errors.append(
            f"Found "
            f"{appid_mismatch} "
            "AppID mismatches."
        )

    if empty_review_text > 0:
        warnings.append(
            f"{empty_review_text} "
            "reviews have empty "
            "review text."
        )

    positive = label_counts[
        "positive"
    ]

    negative = label_counts[
        "negative"
    ]

    labeled = (
        positive
        + negative
    )

    positive_rate = (
        positive / labeled
        if labeled
        else None
    )

    result = (
        "PASS"
        if not errors
        else "FAIL"
    )

    report = {
        "result": result,
        "expected_games": (
            TARGET_GAME_COUNT
        ),
        "review_files": (
            len(review_files)
        ),
        "reviews_per_game_target": (
            TARGET_REVIEWS_PER_GAME
        ),
        "total_records": (
            total_records
        ),
        "unique_recommendation_ids": (
            len(global_ids)
        ),
        "duplicate_ids": (
            duplicate_ids
        ),
        "language_counts": dict(
            language_counts
        ),
        "label_counts": dict(
            label_counts
        ),
        "positive_rate": (
            positive_rate
        ),
        "empty_review_text": (
            empty_review_text
        ),
        "missing_recommendation_id": (
            missing_recommendation_id
        ),
        "missing_voted_up": (
            missing_voted_up
        ),
        "missing_appid": (
            missing_appid
        ),
        "appid_mismatch": (
            appid_mismatch
        ),
        "per_game_counts": (
            per_game_counts
        ),
        "errors": errors,
        "warnings": warnings,
    }

    (
        BRONZE_READY_VALIDATION_REPORT_PATH
        .parent
        .mkdir(
            parents=True,
            exist_ok=True,
        )
    )

    (
        BRONZE_READY_VALIDATION_REPORT_PATH
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
        "STEAM BRONZE-READY VALIDATION"
    )
    print("=" * 80)

    print(
        "Selected games          :",
        len(selected_appids),
    )

    print(
        "Review files            :",
        len(review_files),
    )

    print(
        "Reviews / game          :",
        TARGET_REVIEWS_PER_GAME,
    )

    print(
        "Total review records    :",
        total_records,
    )

    print(
        "Unique recommendationId :",
        len(global_ids),
    )

    print(
        "Duplicate IDs           :",
        duplicate_ids,
    )

    print()
    print("=== LANGUAGE ===")

    for language, count in (
        language_counts.most_common()
    ):
        print(
            language.ljust(20),
            count,
        )

    print()
    print(
        "=== LABEL DISTRIBUTION ==="
    )

    print(
        "Positive               :",
        positive,
    )

    print(
        "Negative               :",
        negative,
    )

    if positive_rate is not None:
        print(
            "Positive rate          :",
            round(
                positive_rate,
                4,
            ),
        )

    print()
    print("=== DATA QUALITY ===")

    print(
        "Empty review text       :",
        empty_review_text,
    )

    print(
        "Missing voted_up        :",
        missing_voted_up,
    )

    print(
        "Missing recommendationId:",
        missing_recommendation_id,
    )

    print(
        "Missing appid           :",
        missing_appid,
    )

    print(
        "AppID mismatch          :",
        appid_mismatch,
    )

    print()
    print("=" * 80)

    print(
        "BRONZE-READY VALIDATION:",
        result,
    )

    if errors:
        print()
        print("ERRORS:")

        for error in errors:
            print(
                "-",
                error,
            )

    if warnings:
        print()
        print("WARNINGS:")

        for warning in warnings:
            print(
                "-",
                warning,
            )

    print()
    print("Report:")
    print(
        BRONZE_READY_VALIDATION_REPORT_PATH
    )


if __name__ == "__main__":
    main()