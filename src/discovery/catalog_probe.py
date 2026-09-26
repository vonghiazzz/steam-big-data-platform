import argparse
import json
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from common.config import (
    CATALOG_PROBE_ROOT,
)
from common.jsonl import (
    write_jsonl,
)


BASE_URL = (
    "https://store.steampowered.com/"
    "search/results/"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 "
        "BDA501-Educational-Project"
    ),
    "Referer": (
        "https://store.steampowered.com/"
        "search/?term="
    ),
}

DEFAULT_PAGE_SIZE = 50
DEFAULT_MAX_PAGES = 4
DEFAULT_DELAY_SECONDS = 1.0


def parse_results_html(
    results_html: str,
    start: int,
) -> list[dict]:
    soup = BeautifulSoup(
        results_html,
        "html.parser",
    )

    rows = []

    for item in soup.select(
        "a.search_result_row"
    ):
        appid_raw = item.get(
            "data-ds-appid"
        )

        if not appid_raw:
            continue

        try:
            appid = int(
                appid_raw
            )
        except ValueError:
            continue

        title_node = item.select_one(
            ".title"
        )

        release_node = item.select_one(
            ".search_released"
        )

        review_node = item.select_one(
            ".search_review_summary"
        )

        rows.append(
            {
                "appid": appid,
                "title": (
                    title_node.get_text(
                        " ",
                        strip=True,
                    )
                    if title_node
                    else None
                ),
                "search_release_text": (
                    release_node.get_text(
                        " ",
                        strip=True,
                    )
                    if release_node
                    else None
                ),
                "search_review_summary": (
                    review_node.get(
                        "data-tooltip-html"
                    )
                    if review_node
                    else None
                ),
                "catalog_start": start,
            }
        )

    return rows


def probe_catalog(
    output_dir: Path,
    page_size: int,
    max_pages: int,
    delay: float,
) -> None:
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    candidates_path = (
        output_dir
        / "candidates.jsonl"
    )

    all_candidates = []
    seen_appids = set()

    total_received = 0
    duplicate_count = 0
    reported_total_count = None

    print("=" * 80)
    print(
        "STEAM CATALOG PROBE"
    )
    print("=" * 80)

    for page_index in range(
        max_pages
    ):
        start = (
            page_index
            * page_size
        )

        params = {
            "query": "",
            "start": start,
            "count": page_size,
            "dynamic_data": "",
            "sort_by": "_ASC",
            "ntype": 0,
            "supportedlang": (
                "english"
            ),
            "infinite": 1,
        }

        response = requests.get(
            BASE_URL,
            params=params,
            headers=HEADERS,
            timeout=30,
        )

        print()
        print(
            f"=== BATCH "
            f"{page_index + 1} ==="
        )

        print(
            "HTTP          :",
            response.status_code,
        )

        print(
            "Start         :",
            start,
        )

        if response.status_code in (
            403,
            429,
            503,
        ):
            raise RuntimeError(
                "Steam returned HTTP "
                f"{response.status_code}. "
                "Stop probing."
            )

        response.raise_for_status()

        payload = (
            response.json()
        )

        if payload.get(
            "success"
        ) != 1:
            raise RuntimeError(
                "Steam search returned "
                f"success="
                f"{payload.get('success')}"
            )

        page_path = (
            output_dir
            / f"page_{start:04d}.json"
        )

        page_path.write_text(
            json.dumps(
                payload,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        if (
            reported_total_count
            is None
        ):
            reported_total_count = (
                payload.get(
                    "total_count"
                )
            )

        rows = (
            parse_results_html(
                payload.get(
                    "results_html",
                    "",
                ),
                start,
            )
        )

        total_received += len(
            rows
        )

        new_count = 0

        for row in rows:
            appid = row[
                "appid"
            ]

            if (
                appid
                in seen_appids
            ):
                duplicate_count += 1
                continue

            seen_appids.add(
                appid
            )

            all_candidates.append(
                row
            )

            new_count += 1

        print(
            "Rows returned  :",
            len(rows),
        )

        print(
            "New unique     :",
            new_count,
        )

        print(
            "Duplicates     :",
            len(rows)
            - new_count,
        )

        time.sleep(
            delay
        )

    write_jsonl(
        candidates_path,
        all_candidates,
    )

    print()
    print("=" * 80)
    print(
        "FINAL VALIDATION"
    )
    print("=" * 80)

    print(
        "Catalog total  :",
        reported_total_count,
    )

    print(
        "Rows received  :",
        total_received,
    )

    print(
        "Unique AppIDs  :",
        len(all_candidates),
    )

    print(
        "Duplicates     :",
        duplicate_count,
    )

    print()
    print(
        "=== FIRST 10 CANDIDATES ==="
    )

    for row in (
        all_candidates[:10]
    ):
        print(
            row["appid"],
            "|",
            row["title"],
            "|",
            row[
                "search_release_text"
            ],
        )

    print()
    print("Output:")
    print(
        candidates_path
    )


def main():
    parser = (
        argparse.ArgumentParser()
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=(
            CATALOG_PROBE_ROOT
        ),
    )

    parser.add_argument(
        "--page-size",
        type=int,
        default=(
            DEFAULT_PAGE_SIZE
        ),
    )

    parser.add_argument(
        "--max-pages",
        type=int,
        default=(
            DEFAULT_MAX_PAGES
        ),
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=(
            DEFAULT_DELAY_SECONDS
        ),
    )

    args = (
        parser.parse_args()
    )

    if args.page_size <= 0:
        parser.error(
            "--page-size must be > 0"
        )

    if args.max_pages <= 0:
        parser.error(
            "--max-pages must be > 0"
        )

    if args.delay < 0:
        parser.error(
            "--delay must be >= 0"
        )

    probe_catalog(
        output_dir=(
            args.output_dir
        ),
        page_size=(
            args.page_size
        ),
        max_pages=(
            args.max_pages
        ),
        delay=args.delay,
    )


if __name__ == "__main__":
    main()
