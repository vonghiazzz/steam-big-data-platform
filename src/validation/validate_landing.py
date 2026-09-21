import json
from collections import Counter

from steam.common.config import (
    LANDING_REVIEWS_ROOT,
    LANDING_STATE_PATH,
    LANDING_VALIDATION_REPORT_PATH,
    SELECTED_GAMES_PATH,
    TARGET_GAME_COUNT,
)
from steam.common.jsonl import iter_jsonl


FINAL_STATUSES = {
    "TARGET_REACHED",
    "EXHAUSTED",
    "EXHAUSTED_STAGNANT",
}


def load_state():
    if not LANDING_STATE_PATH.exists():
        raise RuntimeError(
            f"Missing crawl state: "
            f"{LANDING_STATE_PATH}"
        )

    return json.loads(
        LANDING_STATE_PATH.read_text(
            encoding="utf-8"
        )
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

    state = load_state()

    game_states = state.get(
        "games",
        {},
    )

    review_files = sorted(
        LANDING_REVIEWS_ROOT.glob(
            "*.jsonl"
        )
    )

    file_appids = {
        int(path.stem)
        for path in review_files
    }

    missing_review_files = (
        selected_set
        - file_appids
    )

    extra_review_files = (
        file_appids
        - selected_set
    )

    total_records = 0

    global_review_ids = set()

    duplicate_review_ids = 0

    language_counts = Counter()
    label_counts = Counter()
    reviews_per_game = Counter()

    missing_recommendation_id = 0
    missing_review_text = 0
    missing_voted_up = 0
    missing_appid = 0
    appid_mismatch = 0

    state_count_mismatches = []

    for review_path in review_files:
        file_appid = int(
            review_path.stem
        )

        local_ids = set()

        for row in iter_jsonl(
            review_path
        ):
            total_records += 1

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

            if not recommendation_id:
                missing_recommendation_id += 1

            else:
                recommendation_id = str(
                    recommendation_id
                )

                if (
                    recommendation_id
                    in global_review_ids
                ):
                    duplicate_review_ids += 1

                global_review_ids.add(
                    recommendation_id
                )

                local_ids.add(
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
                missing_review_text += 1

        reviews_per_game[
            file_appid
        ] = len(local_ids)

        game_state = (
            game_states.get(
                str(file_appid),
                {},
            )
        )

        state_unique_reviews = (
            game_state.get(
                "unique_reviews"
            )
        )

        if (
            state_unique_reviews
            is not None
            and state_unique_reviews
            != len(local_ids)
        ):
            state_count_mismatches.append(
                {
                    "appid": file_appid,
                    "state_count": (
                        state_unique_reviews
                    ),
                    "actual_unique": (
                        len(local_ids)
                    ),
                }
            )

    status_counts = Counter()

    unfinished_games = []

    for appid in selected_appids:
        game_state = (
            game_states.get(
                str(appid),
                {},
            )
        )

        status = game_state.get(
            "status",
            "MISSING_STATE",
        )

        status_counts[
            status
        ] += 1

        if status not in FINAL_STATUSES:
            unfinished_games.append(
                {
                    "appid": appid,
                    "status": status,
                }
            )

    non_english_count = sum(
        count
        for language, count
        in language_counts.items()
        if language != "english"
    )

    structural_errors = []

    if (
        len(selected_appids)
        != TARGET_GAME_COUNT
    ):
        structural_errors.append(
            f"Expected "
            f"{TARGET_GAME_COUNT} "
            f"selected games, got "
            f"{len(selected_appids)}."
        )

    if (
        len(selected_set)
        != len(selected_appids)
    ):
        structural_errors.append(
            "Selected manifest "
            "contains duplicate AppIDs."
        )

    if missing_review_files:
        structural_errors.append(
            "Missing review files for "
            f"{len(missing_review_files)} "
            "selected games."
        )

    if extra_review_files:
        structural_errors.append(
            "Found review files for "
            f"{len(extra_review_files)} "
            "unselected games."
        )

    if duplicate_review_ids > 0:
        structural_errors.append(
            f"Found "
            f"{duplicate_review_ids} "
            "duplicate recommendation IDs."
        )

    if non_english_count > 0:
        structural_errors.append(
            f"Found "
            f"{non_english_count} "
            "non-English review records."
        )

    if missing_recommendation_id > 0:
        structural_errors.append(
            f"Found "
            f"{missing_recommendation_id} "
            "records without "
            "recommendationid."
        )

    if missing_appid > 0:
        structural_errors.append(
            f"Found "
            f"{missing_appid} "
            "records without appid."
        )

    if appid_mismatch > 0:
        structural_errors.append(
            f"Found "
            f"{appid_mismatch} "
            "AppID mismatches."
        )

    if unfinished_games:
        structural_errors.append(
            f"Found "
            f"{len(unfinished_games)} "
            "games with unfinished state."
        )

    if state_count_mismatches:
        structural_errors.append(
            f"Found "
            f"{len(state_count_mismatches)} "
            "state/count mismatches."
        )

    data_quality_warnings = []

    if missing_review_text > 0:
        data_quality_warnings.append(
            f"{missing_review_text} "
            "reviews have empty "
            "review text."
        )

    if missing_voted_up > 0:
        data_quality_warnings.append(
            f"{missing_voted_up} "
            "reviews have missing/"
            "invalid voted_up."
        )

    positive_count = (
        label_counts["positive"]
    )

    negative_count = (
        label_counts["negative"]
    )

    labeled_count = (
        positive_count
        + negative_count
    )

    positive_rate = (
        positive_count
        / labeled_count
        if labeled_count
        else None
    )

    review_counts_sorted = sorted(
        reviews_per_game.items(),
        key=lambda item: item[1],
    )

    lowest_review_games = (
        review_counts_sorted[:10]
    )

    highest_review_games = (
        review_counts_sorted[-10:]
    )

    result = (
        "PASS"
        if not structural_errors
        else "FAIL"
    )

    report = {
        "result": result,
        "selected_games": (
            len(selected_appids)
        ),
        "review_files": (
            len(review_files)
        ),
        "total_records": (
            total_records
        ),
        "unique_recommendation_ids": (
            len(global_review_ids)
        ),
        "duplicate_recommendation_ids": (
            duplicate_review_ids
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
        "missing_review_text": (
            missing_review_text
        ),
        "missing_voted_up": (
            missing_voted_up
        ),
        "missing_recommendation_id": (
            missing_recommendation_id
        ),
        "missing_appid": (
            missing_appid
        ),
        "appid_mismatch": (
            appid_mismatch
        ),
        "crawler_status_counts": dict(
            status_counts
        ),
        "missing_review_files": sorted(
            missing_review_files
        ),
        "extra_review_files": sorted(
            extra_review_files
        ),
        "unfinished_games": (
            unfinished_games
        ),
        "state_count_mismatches": (
            state_count_mismatches
        ),
        "lowest_review_games": [
            {
                "appid": appid,
                "reviews": count,
            }
            for appid, count
            in lowest_review_games
        ],
        "highest_review_games": [
            {
                "appid": appid,
                "reviews": count,
            }
            for appid, count
            in highest_review_games
        ],
        "structural_errors": (
            structural_errors
        ),
        "data_quality_warnings": (
            data_quality_warnings
        ),
    }

    (
        LANDING_VALIDATION_REPORT_PATH
        .parent
        .mkdir(
            parents=True,
            exist_ok=True,
        )
    )

    (
        LANDING_VALIDATION_REPORT_PATH
        .write_text(
            json.dumps(
                report,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    )

    print("=" * 80)
    print(
        "STEAM LANDING VALIDATION"
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
        "Total review records    :",
        total_records,
    )

    print(
        "Unique recommendationId :",
        len(global_review_ids),
    )

    print(
        "Duplicate IDs           :",
        duplicate_review_ids,
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
        positive_count,
    )

    print(
        "Negative               :",
        negative_count,
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
    print(
        "=== CRAWLER STATUS ==="
    )

    for status, count in sorted(
        status_counts.items()
    ):
        print(
            status.ljust(25),
            count,
        )

    print()
    print(
        "=== DATA QUALITY ==="
    )

    print(
        "Empty review text       :",
        missing_review_text,
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
    print(
        "=== LOWEST REVIEW COUNTS ==="
    )

    for appid, count in (
        lowest_review_games
    ):
        print(
            appid,
            "→",
            count,
        )

    print()
    print("=" * 80)

    print(
        "INGESTION INTEGRITY:",
        result,
    )

    if structural_errors:
        print()
        print("STRUCTURAL ERRORS:")

        for error in structural_errors:
            print(
                "-",
                error,
            )

    if data_quality_warnings:
        print()
        print(
            "DATA QUALITY WARNINGS:"
        )

        for warning in (
            data_quality_warnings
        ):
            print(
                "-",
                warning,
            )

    print()
    print("Report:")
    print(
        LANDING_VALIDATION_REPORT_PATH
    )


if __name__ == "__main__":
    main()
