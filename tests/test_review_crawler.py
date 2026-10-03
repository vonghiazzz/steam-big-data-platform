import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from src.discovery.policy import RetryPolicy
from src.ingestion.review_crawler import (
    HISTORICAL_REVIEW_FILTER,
    crawl_game,
    request_review_page,
)


class ReviewCrawlerTest(unittest.TestCase):
    def test_historical_request_uses_recent_filter(self):
        response = Mock(status_code=200)
        response.json.return_value = {"success": 1, "reviews": [], "cursor": "x"}
        session = Mock()
        session.get.return_value = response

        _, params = request_review_page(
            session,
            10,
            "*",
            RetryPolicy(max_attempts=1, exponential_backoff=False),
        )

        self.assertEqual("recent", params["filter"])
        self.assertEqual("recent", session.get.call_args.kwargs["params"]["filter"])

    def test_legacy_state_without_filter_restarts_with_current_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reviews_root = root / "reviews"
            pages_root = root / "pages"
            reviews_root.mkdir()
            pages_root.mkdir()
            output = reviews_root / "10.jsonl"
            output.write_text(
                json.dumps(
                    {
                        "appid": 10,
                        "review": {
                            "recommendationid": "old",
                            "language": "english",
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            state = {
                "games": {
                    "10": {
                        "status": "EXHAUSTED",
                        "next_cursor": "legacy-cursor",
                        "next_page_number": 3,
                    }
                }
            }
            payload = {
                "success": 1,
                "cursor": "next",
                "reviews": [
                    {
                        "recommendationid": "new",
                        "language": "english",
                    }
                ],
            }

            with patch(
                "src.ingestion.review_crawler.request_review_page",
                return_value=(
                    payload,
                    {"filter": HISTORICAL_REVIEW_FILTER, "cursor": "*"},
                ),
            ) as request:
                count = crawl_game(
                    session=Mock(),
                    game={"appid": 10, "name": "Ten"},
                    target_reviews=2,
                    delay=0,
                    reviews_root=reviews_root,
                    pages_root=pages_root,
                    state_path=root / "state.json",
                    state=state,
                    retry_policy=RetryPolicy(
                        max_attempts=1,
                        exponential_backoff=False,
                    ),
                )

            self.assertEqual(2, count)
            self.assertEqual("*", request.call_args.args[2])
            self.assertEqual(
                HISTORICAL_REVIEW_FILTER,
                state["games"]["10"]["review_filter"],
            )


if __name__ == "__main__":
    unittest.main()
