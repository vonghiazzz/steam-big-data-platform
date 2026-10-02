from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from typing import Callable

from src.ingestion.review_crawler import (
    MAX_NO_NEW_PAGES,
    request_review_page,
)


PageFetcher = Callable[..., tuple[dict, dict]]


@dataclass(frozen=True)
class ReviewFeasibilityResult:
    appid: int
    target_reviews: int
    unique_reviews: int
    feasible: bool
    status: str
    pages_checked: int

    def to_dict(self) -> dict:
        return asdict(self)


def probe_review_feasibility(
    session,
    appid: int,
    target_reviews: int,
    *,
    delay: float = 0.0,
    retry_policy=None,
    page_fetcher: PageFetcher = request_review_page,
) -> ReviewFeasibilityResult:
    """
    Check whether the historical crawler contract can obtain
    target_reviews unique English reviews for one appid.

    This probe does not write review data.
    """

    if appid <= 0:
        raise ValueError("appid must be positive")

    if target_reviews <= 0:
        raise ValueError("target_reviews must be positive")

    seen_ids: set[str] = set()

    cursor = "*"
    pages_checked = 0
    consecutive_no_new_pages = 0

    status = "UNKNOWN"

    while len(seen_ids) < target_reviews:
        request_cursor = cursor

        payload, _ = page_fetcher(
            session,
            appid,
            request_cursor,
            retry_policy,
        )

        pages_checked += 1

        reviews = payload.get("reviews", [])
        response_cursor = payload.get("cursor")

        before = len(seen_ids)

        for review in reviews:
            if review.get("language") != "english":
                continue

            recommendation_id = review.get(
                "recommendationid"
            )

            if not recommendation_id:
                continue

            seen_ids.add(
                str(recommendation_id)
            )

            if len(seen_ids) >= target_reviews:
                break

        new_reviews = len(seen_ids) - before

        if new_reviews == 0:
            consecutive_no_new_pages += 1
        else:
            consecutive_no_new_pages = 0

        if len(seen_ids) >= target_reviews:
            status = "TARGET_REACHED"
            break

        if not reviews:
            status = "EXHAUSTED"
            break

        if not response_cursor:
            status = "EXHAUSTED"
            break

        if response_cursor == request_cursor:
            status = "EXHAUSTED"
            break

        if (
            consecutive_no_new_pages
            >= MAX_NO_NEW_PAGES
        ):
            status = "EXHAUSTED_STAGNANT"
            break

        cursor = response_cursor

        if delay > 0:
            time.sleep(delay)

    return ReviewFeasibilityResult(
        appid=appid,
        target_reviews=target_reviews,
        unique_reviews=len(seen_ids),
        feasible=(
            len(seen_ids)
            >= target_reviews
        ),
        status=status,
        pages_checked=pages_checked,
    )