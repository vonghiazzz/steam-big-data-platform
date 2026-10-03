import argparse
import json
import time
from pathlib import Path

import requests

from ..common.jsonl import read_jsonl, write_jsonl
from .paths import ELIGIBLE_GAMES_PATH, REVIEW_PROBE_PATH
from .policy import (
    DEFAULT_POLICY_PATH,
    GameQualificationInput,
    evaluate_qualification,
    load_discovery_policy,
)


BASE_URL = (
    "https://store.steampowered.com/"
    "appreviews/{appid}"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 "
        "BDA501-Educational-Project"
    )
}

DEFAULT_REQUEST_DELAY_SECONDS = (
    0.8
)

def _retry_wait_seconds(
    attempt: int,
    *,
    exponential_backoff: bool,
) -> int:
    if not exponential_backoff:
        return 2

    return 2 ** attempt

def _request_with_retry(
    url: str,
    *,
    params: dict,
    headers: dict,
    retry_policy,
):
    last_error = None

    for attempt in range(
        1,
        retry_policy.max_attempts + 1,
    ):
        try:
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=30,
            )

        except requests.RequestException as exc:
            last_error = exc

            if (
                attempt
                >= retry_policy.max_attempts
            ):
                break

            wait_seconds = (
                _retry_wait_seconds(
                    attempt,
                    exponential_backoff=(
                        retry_policy
                        .exponential_backoff
                    ),
                )
            )

            print(
                "NETWORK_ERROR "
                f"retry={attempt}/"
                f"{retry_policy.max_attempts} "
                f"wait={wait_seconds}s",
                end=" ",
                flush=True,
            )

            time.sleep(
                wait_seconds
            )

            continue

        retryable_status = (
            response.status_code == 403
            or response.status_code == 429
            or 500 <= response.status_code < 600
        )

        if not retryable_status:
            response.raise_for_status()
            return response

        last_error = RuntimeError(
            "Steam returned HTTP "
            f"{response.status_code}"
        )

        if (
            attempt
            >= retry_policy.max_attempts
        ):
            break

        retry_after = (
            response.headers.get(
                "Retry-After"
            )
        )

        if retry_after:
            try:
                wait_seconds = max(
                    1,
                    int(retry_after),
                )
            except ValueError:
                wait_seconds = (
                    _retry_wait_seconds(
                        attempt,
                        exponential_backoff=(
                            retry_policy
                            .exponential_backoff
                        ),
                    )
                )
        else:
            wait_seconds = (
                _retry_wait_seconds(
                    attempt,
                    exponential_backoff=(
                        retry_policy
                        .exponential_backoff
                    ),
                )
            )

        print(
            f"HTTP={response.status_code} "
            f"retry={attempt}/"
            f"{retry_policy.max_attempts} "
            f"wait={wait_seconds}s",
            end=" ",
            flush=True,
        )

        time.sleep(
            wait_seconds
        )

    raise RuntimeError(
        "Steam review probe failed after "
        f"{retry_policy.max_attempts} attempts"
    ) from last_error


def build_review_probe_params() -> dict:
    return {
        "json": 1,
        "filter": "recent",
        "language": "english",
        "review_type": "all",
        "purchase_type": "all",
        "num_per_page": 100,
        "cursor": "*",
        "filter_offtopic_activity": 1,
    }


def qualify_reviews(
    input_path: Path,
    output_path: Path,
    delay: float,
    policy_path: Path = DEFAULT_POLICY_PATH,
) -> list[dict]:
    policy = load_discovery_policy(
        policy_path
    )

    games = read_jsonl(
        input_path
    )

    print("=" * 80)
    print(
        "STEAM REVIEW QUALIFICATION"
    )
    print("=" * 80)

    print(
        "Eligible games:",
        len(games),
    )

    success_count = 0
    failed_count = 0

    with_reviews_count = 0
    no_reviews_count = 0

    both_classes_count = 0
    positive_only_count = 0
    negative_only_count = 0

    results = []

    first_query_summary_printed = (
        False
    )

    for index, game in enumerate(
        games,
        start=1,
    ):
        appid = game[
            "appid"
        ]

        name = game[
            "name"
        ]

        url = BASE_URL.format(
            appid=appid
        )

        params = (
            build_review_probe_params()
        )

        print(
            f"[{index:03d}/"
            f"{len(games):03d}] "
            f"{appid} | {name}",
            end=" ",
            flush=True,
        )

        try:
            response = _request_with_retry(
                url,
                params=params,
                headers=HEADERS,
                retry_policy=policy.retry,
            )

            print(
                "HTTP="
                f"{response.status_code}",
                end=" ",
            )

            payload = (
                response.json()
            )

            if payload.get(
                "success"
            ) != 1:
                failed_count += 1

                qualification = (
                    evaluate_qualification(
                        GameQualificationInput(
                            appid=appid,
                            app_type=game.get(
                                "type"
                            ),
                            release_date=(
                                game.get(
                                    "release_date"
                                )
                            ),
                            total_reviews=None,
                            metadata_available=True,
                            review_endpoint_available=False,
                        ),
                        policy.qualification,
                        policy_version=(
                            policy.version
                        ),
                    )
                )

                results.append(
                    {
                        "catalog_rank": (
                            game.get(
                                "catalog_rank"
                            )
                        ),
                        "appid": appid,
                        "name": name,
                        "type": game.get(
                            "type"
                        ),
                        "release_date": (
                            game.get(
                                "release_date"
                            )
                        ),
                        "query_summary": {},
                        "qualification": (
                            qualification
                            .to_dict()
                        ),
                    }
                )

                print(
                    "→ success="
                    f"{payload.get('success')}"
                )

                time.sleep(
                    delay
                )

                continue

            success_count += 1

            reviews = payload.get(
                "reviews",
                [],
            )

            query_summary = (
                payload.get(
                    "query_summary",
                    {},
                )
            )

            if (
                not
                first_query_summary_printed
            ):
                print()
                print()

                print(
                    "=== QUERY SUMMARY "
                    "EXAMPLE ==="
                )

                print(
                    json.dumps(
                        query_summary,
                        ensure_ascii=False,
                        indent=2,
                    )
                )

                print(
                    "=== END QUERY "
                    "SUMMARY ==="
                )

                print()

                first_query_summary_printed = (
                    True
                )

            positive = sum(
                1
                for review in reviews
                if review.get(
                    "voted_up"
                ) is True
            )

            negative = sum(
                1
                for review in reviews
                if review.get(
                    "voted_up"
                ) is False
            )

            review_ids = {
                str(
                    review.get(
                        "recommendationid"
                    )
                )
                for review in reviews
                if review.get(
                    "recommendationid"
                )
            }

            sample_count = len(
                reviews
            )

            if sample_count == 0:
                no_reviews_count += 1

                print(
                    "→ NO ENGLISH REVIEWS"
                )

            else:
                with_reviews_count += 1

                if (
                    positive > 0
                    and negative > 0
                ):
                    both_classes_count += 1

                elif positive > 0:
                    positive_only_count += 1

                elif negative > 0:
                    negative_only_count += 1

                print(
                    f"→ reviews="
                    f"{sample_count} "
                    f"pos={positive} "
                    f"neg={negative}"
                )

            result = {
                "catalog_rank": (
                    game.get(
                        "catalog_rank"
                    )
                ),
                "appid": appid,
                "name": name,
                "type": game.get(
                    "type"
                ),
                "genres": (
                    game.get(
                        "genres",
                        [],
                    )
                ),
                "is_free": (
                    game.get(
                        "is_free"
                    )
                ),
                "release_date": (
                    game.get(
                        "release_date"
                    )
                ),
                "sample_review_count": (
                    sample_count
                ),
                "sample_positive": (
                    positive
                ),
                "sample_negative": (
                    negative
                ),
                "sample_positive_rate": (
                    positive
                    / sample_count
                    if sample_count > 0
                    else None
                ),
                "sample_unique_review_ids": (
                    len(review_ids)
                ),
                "has_cursor": bool(
                    payload.get(
                        "cursor"
                    )
                ),
                "query_summary": (
                    query_summary
                ),
            }

            qualification = (
                evaluate_qualification(
                    GameQualificationInput(
                        appid=appid,
                        app_type=game.get(
                            "type"
                        ),
                        release_date=(
                            game.get(
                                "release_date"
                            )
                        ),
                        total_reviews=(
                            query_summary.get(
                                "total_reviews"
                            )
                        ),
                        metadata_available=True,
                        review_endpoint_available=True,
                    ),
                    policy.qualification,
                    policy_version=(
                        policy.version
                    ),
                )
            )

            result[
                "qualification"
            ] = qualification.to_dict()

            results.append(
                result
            )

        except Exception as exc:
            failed_count += 1

            print()

            print(
                "STOPPED:",
                exc,
            )

            print(
                "Completed results:",
                len(results),
            )

            raise

        time.sleep(
            delay
        )

    write_jsonl(
        output_path,
        results,
    )

    print()
    print("=" * 80)
    print(
        "FINAL VALIDATION"
    )
    print("=" * 80)

    print(
        "Eligible games       :",
        len(games),
    )

    print(
        "Review success       :",
        success_count,
    )

    print(
        "Review failed        :",
        failed_count,
    )

    print(
        "Games with reviews   :",
        with_reviews_count,
    )

    print(
        "Games without reviews:",
        no_reviews_count,
    )

    print(
        "Both classes sample  :",
        both_classes_count,
    )

    print(
        "Positive only sample :",
        positive_only_count,
    )

    print(
        "Negative only sample :",
        negative_only_count,
    )

    print()
    print(
        "=== FIRST 15 REVIEWABLE ==="
    )

    shown = 0

    for row in results:
        if (
            row[
                "sample_review_count"
            ]
            == 0
        ):
            continue

        print(
            row["appid"],
            "|",
            row["name"],
            "| reviews=",
            row[
                "sample_review_count"
            ],
            "| pos=",
            row[
                "sample_positive"
            ],
            "| neg=",
            row[
                "sample_negative"
            ],
            "| rate=",
            round(
                row[
                    "sample_positive_rate"
                ],
                3,
            ),
        )

        shown += 1

        if shown >= 15:
            break

    print()
    print("Output:")
    print(
        output_path
    )
    return results


def main():
    parser = (
        argparse.ArgumentParser()
    )

    parser.add_argument(
        "--input-path",
        type=Path,
        default=(
            ELIGIBLE_GAMES_PATH
        ),
    )

    parser.add_argument(
        "--output-path",
        type=Path,
        default=(
            REVIEW_PROBE_PATH
        ),
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=(
            DEFAULT_REQUEST_DELAY_SECONDS
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

    if args.delay < 0:
        parser.error(
            "--delay must be >= 0"
        )

    qualify_reviews(
        input_path=(
            args.input_path
        ),
        output_path=(
            args.output_path
        ),
        delay=args.delay,
        policy_path=(
            args.policy_path
        ),
    )


if __name__ == "__main__":
    main()
