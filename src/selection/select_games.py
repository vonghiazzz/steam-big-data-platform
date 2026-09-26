import argparse
from collections import Counter, defaultdict
from pathlib import Path

from common.config import (
    REVIEW_PROBE_PATH,
    SELECTED_GAMES_PATH,
    TARGET_GAME_COUNT,
)
from common.jsonl import (
    read_jsonl,
    write_jsonl,
)
from discovery.policy import (
    DEFAULT_POLICY_PATH,
    load_discovery_policy,
)


PREFERRED_QUOTAS = {
    "LOW": 10,
    "MIXED": 15,
    "POSITIVE": 15,
    "HIGH": 10,
}

FILL_ORDER = [
    "POSITIVE",
    "HIGH",
]


def get_bucket(
    positive_rate: float,
) -> str:
    if positive_rate < 0.45:
        return "LOW"

    if positive_rate < 0.60:
        return "MIXED"

    if positive_rate < 0.80:
        return "POSITIVE"

    return "HIGH"


def primary_genre(
    row: dict,
) -> str:
    genres = (
        row.get("genres")
        or []
    )

    if not genres:
        return "Unknown"

    return genres[0]


def qualify_games(
    rows: list[dict],
    min_total_reviews: int | None = None,
) -> list[dict]:
    if min_total_reviews is None:
        min_total_reviews = (
            load_discovery_policy()
            .qualification
            .min_total_reviews
        )

    qualified = []

    for row in rows:
        qualification = row.get(
            "qualification"
        )

        if (
            isinstance(
                qualification,
                dict,
            )
            and qualification.get(
                "qualified"
            )
            is not True
        ):
            continue

        summary = (
            row.get(
                "query_summary"
            )
            or {}
        )

        total_positive = (
            summary.get(
                "total_positive",
                0,
            )
        )

        total_negative = (
            summary.get(
                "total_negative",
                0,
            )
        )

        total_reviews = (
            summary.get(
                "total_reviews",
                0,
            )
        )

        if (
            total_reviews
            < min_total_reviews
        ):
            continue

        positive_rate = (
            total_positive
            / total_reviews
        )

        enriched = dict(
            row
        )

        enriched[
            "total_positive"
        ] = total_positive

        enriched[
            "total_negative"
        ] = total_negative

        enriched[
            "total_reviews"
        ] = total_reviews

        enriched[
            "overall_positive_rate"
        ] = positive_rate

        enriched[
            "selection_bucket"
        ] = get_bucket(
            positive_rate
        )

        enriched[
            "primary_genre"
        ] = primary_genre(
            row
        )

        qualified.append(
            enriched
        )

    return qualified


def group_by_bucket(
    qualified: list[dict],
) -> dict[str, list[dict]]:
    buckets = defaultdict(
        list
    )

    for row in qualified:
        buckets[
            row[
                "selection_bucket"
            ]
        ].append(
            row
        )

    return buckets


def calculate_effective_quotas(
    buckets: dict[
        str,
        list[dict],
    ],
) -> dict[str, int]:
    effective_quotas = {}

    for bucket, preferred in (
        PREFERRED_QUOTAS.items()
    ):
        effective_quotas[
            bucket
        ] = min(
            preferred,
            len(
                buckets[bucket]
            ),
        )

    remaining = (
        TARGET_GAME_COUNT
        - sum(
            effective_quotas.values()
        )
    )

    while remaining > 0:
        progress = False

        for bucket in FILL_ORDER:
            if remaining <= 0:
                break

            available = len(
                buckets[bucket]
            )

            current = (
                effective_quotas[
                    bucket
                ]
            )

            if current >= available:
                continue

            effective_quotas[
                bucket
            ] += 1

            remaining -= 1

            progress = True

        if not progress:
            break

    if remaining > 0:
        raise RuntimeError(
            f"Unable to select "
            f"{TARGET_GAME_COUNT} games. "
            f"Still missing "
            f"{remaining}."
        )

    return effective_quotas


def select_diverse(
    rows: list[dict],
    quota: int,
) -> list[dict]:
    groups = defaultdict(
        list
    )

    for row in rows:
        groups[
            primary_genre(
                row
            )
        ].append(
            row
        )

    for genre in groups:
        groups[genre].sort(
            key=lambda row: (
                row[
                    "total_reviews"
                ]
            ),
            reverse=True,
        )

    genre_order = sorted(
        groups,
        key=lambda genre: (
            groups[genre][0][
                "total_reviews"
            ]
            if groups[genre]
            else 0
        ),
        reverse=True,
    )

    selected = []

    while (
        len(selected)
        < quota
    ):
        progress = False

        for genre in genre_order:
            if (
                len(selected)
                >= quota
            ):
                break

            if not groups[genre]:
                continue

            selected.append(
                groups[
                    genre
                ].pop(0)
            )

            progress = True

        if not progress:
            break

    return selected


def select_games(
    qualified: list[dict],
) -> tuple[
    list[dict],
    dict[str, int],
    dict[str, list[dict]],
]:
    buckets = (
        group_by_bucket(
            qualified
        )
    )

    effective_quotas = (
        calculate_effective_quotas(
            buckets
        )
    )

    selected = []

    for bucket, quota in (
        effective_quotas.items()
    ):
        bucket_selected = (
            select_diverse(
                buckets[bucket],
                quota,
            )
        )

        if (
            len(bucket_selected)
            < quota
        ):
            raise RuntimeError(
                f"Unexpected shortage "
                f"in {bucket}. "
                f"Need {quota}, "
                f"found "
                f"{len(bucket_selected)}."
            )

        selected.extend(
            bucket_selected
        )

    if (
        len(selected)
        != TARGET_GAME_COUNT
    ):
        raise RuntimeError(
            f"Expected "
            f"{TARGET_GAME_COUNT} "
            f"games, got "
            f"{len(selected)}."
        )

    appids = [
        row["appid"]
        for row in selected
    ]

    if (
        len(appids)
        != len(set(appids))
    ):
        raise RuntimeError(
            "Duplicate AppIDs found "
            "in final selection."
        )

    return (
        selected,
        effective_quotas,
        buckets,
    )


def build_output_rows(
    selected: list[dict],
) -> list[dict]:
    output_rows = []

    for row in selected:
        output_rows.append(
            {
                "appid": (
                    row["appid"]
                ),
                "name": (
                    row["name"]
                ),
                "genres": row.get(
                    "genres",
                    [],
                ),
                "primary_genre": (
                    row[
                        "primary_genre"
                    ]
                ),
                "is_free": row.get(
                    "is_free"
                ),
                "release_date": (
                    row.get(
                        "release_date"
                    )
                ),
                "selection_bucket": (
                    row[
                        "selection_bucket"
                    ]
                ),
                "total_reviews": (
                    row[
                        "total_reviews"
                    ]
                ),
                "total_positive": (
                    row[
                        "total_positive"
                    ]
                ),
                "total_negative": (
                    row[
                        "total_negative"
                    ]
                ),
                "overall_positive_rate": (
                    row[
                        "overall_positive_rate"
                    ]
                ),
            }
        )

    return output_rows


def print_report(
    rows: list[dict],
    qualified: list[dict],
    selected: list[dict],
    effective_quotas: dict[
        str,
        int,
    ],
    bucket_availability: dict[
        str,
        int,
    ],
    output_path: Path,
) -> None:
    print("=" * 80)
    print(
        "STEAM 50-GAME SELECTION"
    )
    print("=" * 80)

    print(
        "Review probe rows :",
        len(rows),
    )

    print(
        "Qualified games   :",
        len(qualified),
    )

    print()
    print(
        "=== BUCKET AVAILABILITY ==="
    )

    for bucket in (
        PREFERRED_QUOTAS
    ):
        print(
            bucket.ljust(8),
            ":",
            bucket_availability[
                bucket
            ],
            "available",
            "| preferred =",
            PREFERRED_QUOTAS[
                bucket
            ],
            "| selected =",
            effective_quotas[
                bucket
            ],
        )

    print()
    print("=" * 80)
    print("FINAL SELECTION")
    print("=" * 80)

    bucket_counts = Counter(
        row[
            "selection_bucket"
        ]
        for row in selected
    )

    for bucket in (
        PREFERRED_QUOTAS
    ):
        print(
            bucket.ljust(8),
            ":",
            bucket_counts[
                bucket
            ],
        )

    print()
    print(
        "=== SELECTED 50 GAMES ==="
    )

    for index, row in enumerate(
        selected,
        start=1,
    ):
        print(
            f"{index:02d}",
            "|",
            row[
                "selection_bucket"
            ].ljust(8),
            "|",
            row["appid"],
            "|",
            row["name"],
            "| rate=",
            round(
                row[
                    "overall_positive_rate"
                ],
                3,
            ),
            "| reviews=",
            row[
                "total_reviews"
            ],
            "| genre=",
            row[
                "primary_genre"
            ],
        )

    print()
    print(
        "=== PRIMARY GENRE DISTRIBUTION ==="
    )

    genre_counts = Counter(
        row[
            "primary_genre"
        ]
        for row in selected
    )

    for genre, count in (
        genre_counts.most_common()
    ):
        print(
            genre.ljust(25),
            count,
        )

    appids = {
        row["appid"]
        for row in selected
    }

    print()

    print(
        "Selected games:",
        len(selected),
    )

    print(
        "Unique AppIDs :",
        len(appids),
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
        "--input-path",
        type=Path,
        default=(
            REVIEW_PROBE_PATH
        ),
    )

    parser.add_argument(
        "--output-path",
        type=Path,
        default=(
            SELECTED_GAMES_PATH
        ),
    )

    parser.add_argument(
        "--policy-path",
        type=Path,
        default=DEFAULT_POLICY_PATH,
    )

    args = (
        parser.parse_args()
    )

    rows = read_jsonl(
        args.input_path
    )

    policy = load_discovery_policy(
        args.policy_path
    )

    qualified = (
        qualify_games(
            rows,
            min_total_reviews=(
                policy.qualification
                .min_total_reviews
            ),
        )
    )

    buckets = (
        group_by_bucket(
            qualified
        )
    )

    bucket_availability = {
        bucket: len(
            buckets[bucket]
        )
        for bucket in (
            PREFERRED_QUOTAS
        )
    }

    (
        selected,
        effective_quotas,
        _,
    ) = select_games(
        qualified
    )

    output_rows = (
        build_output_rows(
            selected
        )
    )

    write_jsonl(
        args.output_path,
        output_rows,
    )

    print_report(
        rows=rows,
        qualified=qualified,
        selected=selected,
        effective_quotas=(
            effective_quotas
        ),
        bucket_availability=(
            bucket_availability
        ),
        output_path=(
            args.output_path
        ),
    )


if __name__ == "__main__":
    main()
