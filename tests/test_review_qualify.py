import unittest
from unittest.mock import patch

import requests

from src.discovery.policy import load_discovery_policy
from src.discovery.review_qualify import _request_with_retry


class FakeResponse:
    def __init__(self, status_code, headers=None):
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            response = requests.Response()
            response.status_code = self.status_code
            raise requests.HTTPError(
                f"{self.status_code} error",
                response=response,
            )


class ReviewQualificationRetryTest(unittest.TestCase):
    def setUp(self):
        self.policy = load_discovery_policy()

    def test_retryable_503_recovers(self):
        responses = [
            FakeResponse(503),
            FakeResponse(200),
        ]
        calls = []
        sleeps = []

        def fake_get(*args, **kwargs):
            response = responses.pop(0)
            calls.append(response.status_code)
            return response

        with (
            patch(
                "src.discovery.review_qualify.requests.get",
                side_effect=fake_get,
            ),
            patch(
                "src.discovery.review_qualify.time.sleep",
                side_effect=sleeps.append,
            ),
        ):
            response = _request_with_retry(
                "https://example.invalid",
                params={},
                headers={},
                retry_policy=self.policy.retry,
            )

        self.assertEqual(200, response.status_code)
        self.assertEqual([503, 200], calls)
        self.assertEqual([2], sleeps)

    def test_non_retryable_404_fails_immediately(self):
        calls = []
        sleeps = []

        def fake_get(*args, **kwargs):
            calls.append(404)
            return FakeResponse(404)

        with (
            patch(
                "src.discovery.review_qualify.requests.get",
                side_effect=fake_get,
            ),
            patch(
                "src.discovery.review_qualify.time.sleep",
                side_effect=sleeps.append,
            ),
        ):
            with self.assertRaises(requests.HTTPError):
                _request_with_retry(
                    "https://example.invalid",
                    params={},
                    headers={},
                    retry_policy=self.policy.retry,
                )

        self.assertEqual([404], calls)
        self.assertEqual([], sleeps)


if __name__ == "__main__":
    unittest.main()
