import unittest
from dataclasses import replace
from datetime import date

from src.discovery.policy import (
    GAME_TOO_NEW,
    INSUFFICIENT_REVIEWS,
    INVALID_APP_TYPE,
    INVALID_RELEASE_DATE,
    METADATA_UNAVAILABLE,
    REVIEW_ENDPOINT_UNAVAILABLE,
    GameQualificationInput,
    QualificationPolicy,
    evaluate_qualification,
    load_discovery_policy,
)


AS_OF_DATE = date(2026, 9, 22)


class QualificationPolicyTest(unittest.TestCase):
    def setUp(self):
        self.policy = QualificationPolicy(
            app_type="game",
            min_release_age_days=30,
            min_total_reviews=1000,
            require_metadata=True,
            require_review_endpoint=True,
            min_playtime_minutes=None,
        )
        self.valid_game = GameQualificationInput(
            appid=123456,
            app_type="game",
            release_date="2025-09-22",
            total_reviews=2000,
            metadata_available=True,
            review_endpoint_available=True,
            playtime_minutes=0,
        )

    def evaluate(self, game):
        return evaluate_qualification(
            game,
            self.policy,
            policy_version=1,
            current_date=AS_OF_DATE,
        )

    def reason_codes(self, result):
        return {reason.rule for reason in result.reasons}

    def test_valid_game_passes(self):
        result = self.evaluate(self.valid_game)
        self.assertTrue(result.qualified)
        self.assertEqual((), result.reasons)

    def test_non_game_app_fails(self):
        result = self.evaluate(replace(self.valid_game, app_type="dlc"))
        self.assertFalse(result.qualified)
        self.assertIn(INVALID_APP_TYPE, self.reason_codes(result))

    def test_game_younger_than_30_days_fails(self):
        result = self.evaluate(
            replace(self.valid_game, release_date="2026-08-24")
        )
        self.assertIn(GAME_TOO_NEW, self.reason_codes(result))

    def test_missing_release_date_fails_explicitly(self):
        result = self.evaluate(
            replace(self.valid_game, release_date=None)
        )
        self.assertIn(INVALID_RELEASE_DATE, self.reason_codes(result))

    def test_game_below_1000_reviews_fails(self):
        result = self.evaluate(replace(self.valid_game, total_reviews=999))
        self.assertIn(INSUFFICIENT_REVIEWS, self.reason_codes(result))

    def test_missing_metadata_fails_when_required(self):
        result = self.evaluate(
            replace(self.valid_game, metadata_available=False)
        )
        self.assertIn(METADATA_UNAVAILABLE, self.reason_codes(result))

    def test_unavailable_review_endpoint_fails(self):
        result = self.evaluate(
            replace(self.valid_game, review_endpoint_available=False)
        )
        self.assertIn(REVIEW_ENDPOINT_UNAVAILABLE, self.reason_codes(result))

    def test_release_age_boundary_30_days_passes(self):
        result = self.evaluate(
            replace(self.valid_game, release_date="2026-08-23")
        )
        self.assertTrue(result.qualified)
        self.assertEqual(30, result.release_age_days)

    def test_total_reviews_boundary_1000_passes(self):
        result = self.evaluate(replace(self.valid_game, total_reviews=1000))
        self.assertTrue(result.qualified)

    def test_disabled_playtime_rule_does_not_filter(self):
        result = self.evaluate(
            replace(self.valid_game, playtime_minutes=None)
        )
        self.assertTrue(result.qualified)

    def test_multiple_failures_return_multiple_reason_codes(self):
        result = self.evaluate(
            GameQualificationInput(
                appid=654321,
                app_type="dlc",
                release_date="2026-09-20",
                total_reviews=320,
                metadata_available=False,
                review_endpoint_available=False,
            )
        )
        self.assertEqual(
            {
                INVALID_APP_TYPE,
                GAME_TOO_NEW,
                INSUFFICIENT_REVIEWS,
                METADATA_UNAVAILABLE,
                REVIEW_ENDPOINT_UNAVAILABLE,
            },
            self.reason_codes(result),
        )

    def test_repository_policy_defaults_load(self):
        policy = load_discovery_policy()
        self.assertEqual(1, policy.version)
        self.assertEqual(1000, policy.qualification.min_total_reviews)
        self.assertIsNone(policy.qualification.min_playtime_minutes)
        self.assertEqual(500, policy.onboarding.target_reviews_per_game)
        self.assertEqual(10, policy.onboarding.max_new_games_per_cycle)
        self.assertEqual("WEEKLY", policy.discovery.frequency)
        self.assertEqual(3, policy.retry.max_attempts)


if __name__ == "__main__":
    unittest.main()
