import argparse
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from common.config import (
    LANDING_ROOT,
    SELECTED_GAMES_PATH,
)
from common.jsonl import read_jsonl
from discovery.policy import (
    DEFAULT_POLICY_PATH,
    RetryPolicy,
    load_discovery_policy,
)


BASE_URL = (
    "https://store.steampowered.com/"
    "appreviews/{appid}"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 BDA501-Educational-Project"
}

MAX_NO_NEW_PAGES = 3


def utc_now():
    return (
        datetime.now(timezone.utc)
        .isoformat()
    )


def load_state(path):
    if not path.exists():
        return {
            "games": {}
        }

    return json.loads(
        path.read_text(
            encoding="utf-8"
        )
    )


def save_state(path, state):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    tmp_path = path.with_suffix(
        ".tmp"
    )

    tmp_path.write_text(
        json.dumps(
            state,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    tmp_path.replace(path)


def load_seen_review_ids(path):
    seen = set()

    if not path.exists():
        return seen

    with path.open(
        "r",
        encoding="utf-8",
    ) as file:
        for line in file:
            line = line.strip()

            if not line:
                continue

            row = json.loads(
                line
            )

            review = (
                row.get("review")
                or {}
            )

            recommendation_id = (
                review.get(
                    "recommendationid"
                )
            )

            if recommendation_id:
                seen.add(
                    str(
                        recommendation_id
                    )
                )

    return seen


def request_review_page(
    session,
    appid,
    cursor,
    retry_policy=None,
):
    if retry_policy is None:
        retry_policy = (
            load_discovery_policy()
            .retry
        )

    url = BASE_URL.format(
        appid=appid
    )

    params = {
        "json": 1,
        "filter": "recent",
        "language": "english",
        "review_type": "all",
        "purchase_type": "all",
        "num_per_page": 100,
        "cursor": cursor,
        "filter_offtopic_activity": 1,
    }

    for attempt in range(
        1,
        retry_policy.max_attempts + 1,
    ):
        try:
            response = session.get(
                url,
                params=params,
                timeout=30,
            )
        except requests.RequestException as exc:
            if attempt >= retry_policy.max_attempts:
                raise RuntimeError(
                    "Steam request failed after "
                    f"{retry_policy.max_attempts} attempts."
                ) from exc

            wait_seconds = retry_wait_seconds(
                attempt,
                retry_policy,
            )

            print()
            print(
                "Network error. "
                f"Retry in {wait_seconds}s..."
            )

            time.sleep(wait_seconds)
            continue

        if response.status_code == 200:
            payload = response.json()

            if payload.get(
                "success"
            ) != 1:
                raise RuntimeError(
                    f"Steam success="
                    f"{payload.get('success')}"
                )

            return payload, params

        if response.status_code == 403:
            raise RuntimeError(
                "Steam returned HTTP 403. "
                "Stop crawl."
            )

        if response.status_code == 429:
            retry_after = (
                response.headers.get(
                    "Retry-After"
                )
            )

            wait_seconds = retry_wait_seconds(
                attempt,
                retry_policy,
            )

            if (
                retry_after
                and retry_after.isdigit()
            ):
                wait_seconds = int(
                    retry_after
                )

            print()
            print(
                f"HTTP 429. "
                f"Waiting {wait_seconds}s..."
            )

            if attempt < retry_policy.max_attempts:
                time.sleep(
                    wait_seconds
                )

            continue

        if response.status_code >= 500:
            wait_seconds = retry_wait_seconds(
                attempt,
                retry_policy,
            )

            print()
            print(
                f"HTTP "
                f"{response.status_code}. "
                f"Retry in "
                f"{wait_seconds}s..."
            )

            if attempt < retry_policy.max_attempts:
                time.sleep(
                    wait_seconds
                )

            continue

        response.raise_for_status()

    raise RuntimeError(
        f"Failed after "
        f"{retry_policy.max_attempts} attempts."
    )


def retry_wait_seconds(
    attempt: int,
    retry_policy: RetryPolicy,
) -> int:
    if retry_policy.exponential_backoff:
        return 2 ** attempt
    return 2


def save_raw_page(
    page_root,
    appid,
    game_name,
    page_number,
    request_cursor,
    request_params,
    payload,
):
    game_dir = (
        page_root
        / str(appid)
    )

    game_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    page_path = (
        game_dir
        / f"page_{page_number:04d}.json"
    )

    wrapper = {
        "appid": appid,
        "game_name": game_name,
        "collected_at": utc_now(),
        "page_number": page_number,
        "request_cursor": request_cursor,
        "request_params": request_params,
        "payload": payload,
    }

    page_path.write_text(
        json.dumps(
            wrapper,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def crawl_game(
    session,
    game,
    target_reviews,
    delay,
    reviews_root,
    pages_root,
    state_path,
    state,
    retry_policy=None,
):
    if retry_policy is None:
        retry_policy = (
            load_discovery_policy()
            .retry
        )

    appid = game["appid"]
    name = game["name"]

    output_path = (
        reviews_root
        / f"{appid}.jsonl"
    )

    seen_ids = (
        load_seen_review_ids(
            output_path
        )
    )

    game_key = str(
        appid
    )

    game_state = (
        state["games"].get(
            game_key,
            {},
        )
    )

    if game_state.get(
        "status"
    ) in (
        "EXHAUSTED",
        "EXHAUSTED_STAGNANT",
    ):
        print(
            f"SKIP exhausted: "
            f"{appid} | {name} "
            f"({len(seen_ids)} unique)"
        )

        return len(
            seen_ids
        )

    if (
        len(seen_ids)
        >= target_reviews
    ):
        print(
            f"SKIP target already met: "
            f"{appid} | {name} "
            f"({len(seen_ids)})"
        )

        return len(
            seen_ids
        )

    cursor = game_state.get(
        "next_cursor",
        "*",
    )

    page_number = game_state.get(
        "next_page_number",
        1,
    )

    print()
    print("=" * 80)

    print(
        f"{appid} | {name}"
    )

    print(
        f"Existing unique reviews: "
        f"{len(seen_ids)}"
    )

    print(
        f"Resume cursor: {cursor}"
    )

    consecutive_no_new_pages = 0

    while (
        len(seen_ids)
        < target_reviews
    ):
        request_cursor = cursor

        payload, request_params = (
            request_review_page(
                session,
                appid,
                request_cursor,
                retry_policy,
            )
        )

        reviews = payload.get(
            "reviews",
            [],
        )

        response_cursor = (
            payload.get(
                "cursor"
            )
        )

        save_raw_page(
            pages_root,
            appid,
            name,
            page_number,
            request_cursor,
            request_params,
            payload,
        )

        new_reviews = 0
        duplicates = 0
        non_english = 0

        with output_path.open(
            "a",
            encoding="utf-8",
        ) as file:
            for review in reviews:

                if (
                    len(seen_ids)
                    >= target_reviews
                ):
                    break

                language = review.get(
                    "language"
                )

                if language != "english":
                    non_english += 1
                    continue

                recommendation_id = (
                    review.get(
                        "recommendationid"
                    )
                )

                if not recommendation_id:
                    continue

                recommendation_id = str(
                    recommendation_id
                )

                if (
                    recommendation_id
                    in seen_ids
                ):
                    duplicates += 1
                    continue

                record = {
                    "appid": appid,
                    "game_name": name,
                    "ingested_at": utc_now(),
                    "page_number": (
                        page_number
                    ),
                    "request_cursor": (
                        request_cursor
                    ),
                    "review": review,
                }

                file.write(
                    json.dumps(
                        record,
                        ensure_ascii=False,
                    )
                )

                file.write(
                    "\n"
                )

                seen_ids.add(
                    recommendation_id
                )

                new_reviews += 1

        if new_reviews == 0:
            consecutive_no_new_pages += 1
        else:
            consecutive_no_new_pages = 0

        print(
            f"page={page_number:03d}"
            f" received={len(reviews)}"
            f" new={new_reviews}"
            f" duplicates={duplicates}"
            f" non_english={non_english}"
            f" total={len(seen_ids)}"
        )

        if not reviews:
            status = "EXHAUSTED"

        elif not response_cursor:
            status = "EXHAUSTED"

        elif (
            response_cursor
            == request_cursor
        ):
            status = "EXHAUSTED"

        elif (
            len(seen_ids)
            >= target_reviews
        ):
            status = (
                "TARGET_REACHED"
            )

        elif (
            consecutive_no_new_pages
            >= MAX_NO_NEW_PAGES
        ):
            status = (
                "EXHAUSTED_STAGNANT"
            )

            print(
                f"STOP STAGNANT: "
                f"{consecutive_no_new_pages} "
                f"consecutive pages "
                f"with 0 new reviews. "
                f"Keeping "
                f"{len(seen_ids)} "
                f"unique reviews."
            )

        else:
            status = "IN_PROGRESS"

        state["games"][
            game_key
        ] = {
            "appid": appid,
            "name": name,
            "status": status,
            "unique_reviews": (
                len(seen_ids)
            ),
            "last_page_number": (
                page_number
            ),
            "next_page_number": (
                page_number + 1
            ),
            "next_cursor": (
                response_cursor
            ),
            "updated_at": utc_now(),
        }

        save_state(
            state_path,
            state,
        )

        if status in (
            "TARGET_REACHED",
            "EXHAUSTED",
            "EXHAUSTED_STAGNANT",
        ):
            break

        cursor = response_cursor

        page_number += 1

        sleep_seconds = (
            delay
            + random.uniform(
                0.0,
                0.4,
            )
        )

        time.sleep(
            sleep_seconds
        )

    print(
        f"Finished: {appid} | "
        f"{name} | "
        f"{len(seen_ids)} unique"
    )

    return len(
        seen_ids
    )


def main():
    parser = (
        argparse.ArgumentParser()
    )

    parser.add_argument(
        "--target-reviews",
        type=int,
        default=None,
    )

    parser.add_argument(
        "--policy-path",
        type=Path,
        default=DEFAULT_POLICY_PATH,
    )

    parser.add_argument(
        "--games-path",
        type=Path,
        default=SELECTED_GAMES_PATH,
        help=(
            "JSONL game input. Defaults to the fixed research snapshot; "
            "a discovery onboarding queue may be supplied explicitly."
        ),
    )

    parser.add_argument(
        "--max-games",
        type=int,
        default=None,
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
    )

    parser.add_argument(
        "--output-root",
        type=Path,
        default=LANDING_ROOT,
    )

    args = (
        parser.parse_args()
    )

    policy = load_discovery_policy(
        args.policy_path
    )

    if args.target_reviews is None:
        args.target_reviews = (
            policy.onboarding
            .target_reviews_per_game
        )

    if args.target_reviews <= 0:
        parser.error(
            "--target-reviews "
            "must be greater than 0"
        )

    if (
        args.max_games is not None
        and args.max_games <= 0
    ):
        parser.error(
            "--max-games "
            "must be greater than 0"
        )

    if args.delay < 0:
        parser.error(
            "--delay must be "
            "greater than or equal to 0"
        )

    games = read_jsonl(
        args.games_path
    )

    if args.max_games is not None:
        games = games[
            :args.max_games
        ]

    output_root = (
        args.output_root
    )

    reviews_root = (
        output_root
        / "reviews_by_game"
    )

    pages_root = (
        output_root
        / "review_pages"
    )

    state_path = (
        output_root
        / "crawl_state.json"
    )

    reviews_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    pages_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    state = load_state(
        state_path
    )

    session = (
        requests.Session()
    )

    session.headers.update(
        HEADERS
    )

    print("=" * 80)
    print(
        "STEAM PRODUCTION REVIEW CRAWLER"
    )
    print("=" * 80)

    print(
        "Games this run      :",
        len(games),
    )

    print(
        "Target reviews/game :",
        args.target_reviews,
    )

    print(
        "Delay               :",
        args.delay,
    )

    print(
        "Policy version      :",
        policy.version,
    )

    print(
        "Max retry attempts  :",
        policy.retry.max_attempts,
    )

    total_stored = 0

    for index, game in enumerate(
        games,
        start=1,
    ):
        print()

        print(
            f"[GAME "
            f"{index}/{len(games)}]"
        )

        try:
            count = crawl_game(
                session=session,
                game=game,
                target_reviews=(
                    args.target_reviews
                ),
                delay=args.delay,
                reviews_root=(
                    reviews_root
                ),
                pages_root=(
                    pages_root
                ),
                state_path=(
                    state_path
                ),
                state=state,
                retry_policy=(
                    policy.retry
                ),
            )
        except Exception as exc:
            game_key = str(
                game["appid"]
            )

            failed_state = dict(
                state["games"].get(
                    game_key,
                    {},
                )
            )

            failed_state.update(
                {
                    "appid": game["appid"],
                    "name": game["name"],
                    "status": "FAILED",
                    "error": str(exc),
                    "policy_version": (
                        policy.version
                    ),
                    "updated_at": utc_now(),
                }
            )

            state["games"][
                game_key
            ] = failed_state

            save_state(
                state_path,
                state,
            )

            print(
                "FAILED game; continuing:",
                game["appid"],
                "|",
                exc,
            )

            continue

        total_stored += (
            count
        )

        time.sleep(
            args.delay
        )

    print()
    print("=" * 80)
    print("RUN COMPLETE")
    print("=" * 80)

    print(
        "Games processed :",
        len(games),
    )

    print(
        "Stored reviews  :",
        total_stored,
    )

    print(
        "State file      :",
        state_path,
    )

    print(
        "Reviews folder  :",
        reviews_root,
    )

    print(
        "Raw pages folder:",
        pages_root,
    )


if __name__ == "__main__":
    main()
