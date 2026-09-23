"""Configuration model and evaluator for Steam game qualification policy."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping


DEFAULT_POLICY_PATH = (
    Path(__file__).resolve().parents[2]
    / "config"
    / "discovery_policy.json"
)

INVALID_APP_TYPE = "INVALID_APP_TYPE"
GAME_TOO_NEW = "GAME_TOO_NEW"
INVALID_RELEASE_DATE = "INVALID_RELEASE_DATE"
INSUFFICIENT_REVIEWS = "INSUFFICIENT_REVIEWS"
METADATA_UNAVAILABLE = "METADATA_UNAVAILABLE"
REVIEW_ENDPOINT_UNAVAILABLE = "REVIEW_ENDPOINT_UNAVAILABLE"
PLAYTIME_UNAVAILABLE = "PLAYTIME_UNAVAILABLE"
INSUFFICIENT_PLAYTIME = "INSUFFICIENT_PLAYTIME"

RELEASE_DATE_FORMATS = (
    "%Y-%m-%d",
    "%d %b, %Y",
    "%b %d, %Y",
)


@dataclass(frozen=True)
class QualificationPolicy:
    app_type: str
    min_release_age_days: int
    min_total_reviews: int
    require_metadata: bool
    require_review_endpoint: bool
    min_playtime_minutes: int | None = None


@dataclass(frozen=True)
class OnboardingPolicy:
    target_reviews_per_game: int
    max_new_games_per_cycle: int


@dataclass(frozen=True)
class DiscoverySchedulePolicy:
    frequency: str


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int
    exponential_backoff: bool


@dataclass(frozen=True)
class DiscoveryPolicy:
    version: int
    qualification: QualificationPolicy
    onboarding: OnboardingPolicy
    discovery: DiscoverySchedulePolicy
    retry: RetryPolicy


@dataclass(frozen=True)
class GameQualificationInput:
    appid: int
    app_type: str | None
    release_date: date | datetime | str | None
    total_reviews: int | str | None
    metadata_available: bool
    review_endpoint_available: bool
    playtime_minutes: int | float | None = None


@dataclass(frozen=True)
class QualificationReason:
    rule: str
    expected: Any
    actual: Any


@dataclass(frozen=True)
class QualificationResult:
    appid: int
    qualified: bool
    policy_version: int
    release_age_days: int | None
    reasons: tuple[QualificationReason, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "appid": self.appid,
            "qualified": self.qualified,
            "policy_version": self.policy_version,
            "release_age_days": self.release_age_days,
            "reasons": [asdict(reason) for reason in self.reasons],
        }


def load_discovery_policy(
    path: Path = DEFAULT_POLICY_PATH,
) -> DiscoveryPolicy:
    """Load and validate the versioned discovery policy JSON file."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Discovery policy file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid discovery policy JSON in {path}: {exc}") from exc

    root = _require_mapping(payload, "policy")
    qualification = _require_mapping(
        root.get("qualification"), "qualification"
    )
    onboarding = _require_mapping(root.get("onboarding"), "onboarding")
    discovery = _require_mapping(root.get("discovery"), "discovery")
    retry = _require_mapping(root.get("retry"), "retry")

    min_playtime = qualification.get("min_playtime_minutes")
    if min_playtime is not None:
        min_playtime = _require_non_negative_int(
            min_playtime, "qualification.min_playtime_minutes"
        )

    policy = DiscoveryPolicy(
        version=_require_positive_int(root.get("version"), "version"),
        qualification=QualificationPolicy(
            app_type=_require_non_empty_string(
                qualification.get("app_type"), "qualification.app_type"
            ),
            min_release_age_days=_require_non_negative_int(
                qualification.get("min_release_age_days"),
                "qualification.min_release_age_days",
            ),
            min_total_reviews=_require_non_negative_int(
                qualification.get("min_total_reviews"),
                "qualification.min_total_reviews",
            ),
            require_metadata=_require_bool(
                qualification.get("require_metadata"),
                "qualification.require_metadata",
            ),
            require_review_endpoint=_require_bool(
                qualification.get("require_review_endpoint"),
                "qualification.require_review_endpoint",
            ),
            min_playtime_minutes=min_playtime,
        ),
        onboarding=OnboardingPolicy(
            target_reviews_per_game=_require_positive_int(
                onboarding.get("target_reviews_per_game"),
                "onboarding.target_reviews_per_game",
            ),
            max_new_games_per_cycle=_require_positive_int(
                onboarding.get("max_new_games_per_cycle"),
                "onboarding.max_new_games_per_cycle",
            ),
        ),
        discovery=DiscoverySchedulePolicy(
            frequency=_require_non_empty_string(
                discovery.get("frequency"), "discovery.frequency"
            ).upper(),
        ),
        retry=RetryPolicy(
            max_attempts=_require_positive_int(
                retry.get("max_attempts"), "retry.max_attempts"
            ),
            exponential_backoff=_require_bool(
                retry.get("exponential_backoff"),
                "retry.exponential_backoff",
            ),
        ),
    )

    return policy


def evaluate_qualification(
    game: GameQualificationInput,
    policy: QualificationPolicy,
    *,
    policy_version: int,
    current_date: date | None = None,
) -> QualificationResult:
    """Evaluate all enabled rules and return every failure reason."""
    as_of = current_date or date.today()
    reasons: list[QualificationReason] = []

    if game.app_type != policy.app_type:
        reasons.append(
            QualificationReason(
                rule=INVALID_APP_TYPE,
                expected=policy.app_type,
                actual=game.app_type,
            )
        )

    parsed_release_date = parse_release_date(game.release_date)
    release_age_days: int | None = None
    if parsed_release_date is None:
        reasons.append(
            QualificationReason(
                rule=INVALID_RELEASE_DATE,
                expected="valid release date",
                actual=_serializable_value(game.release_date),
            )
        )
    else:
        release_age_days = (as_of - parsed_release_date).days
        if release_age_days < policy.min_release_age_days:
            reasons.append(
                QualificationReason(
                    rule=GAME_TOO_NEW,
                    expected=policy.min_release_age_days,
                    actual=release_age_days,
                )
            )

    total_reviews = _coerce_int(game.total_reviews)
    if total_reviews is None or total_reviews < policy.min_total_reviews:
        reasons.append(
            QualificationReason(
                rule=INSUFFICIENT_REVIEWS,
                expected=policy.min_total_reviews,
                actual=total_reviews,
            )
        )

    if policy.require_metadata and game.metadata_available is not True:
        reasons.append(
            QualificationReason(
                rule=METADATA_UNAVAILABLE,
                expected=True,
                actual=game.metadata_available,
            )
        )

    if (
        policy.require_review_endpoint
        and game.review_endpoint_available is not True
    ):
        reasons.append(
            QualificationReason(
                rule=REVIEW_ENDPOINT_UNAVAILABLE,
                expected=True,
                actual=game.review_endpoint_available,
            )
        )

    if policy.min_playtime_minutes is not None:
        playtime = _coerce_number(game.playtime_minutes)
        if playtime is None:
            reasons.append(
                QualificationReason(
                    rule=PLAYTIME_UNAVAILABLE,
                    expected=policy.min_playtime_minutes,
                    actual=None,
                )
            )
        elif playtime < policy.min_playtime_minutes:
            reasons.append(
                QualificationReason(
                    rule=INSUFFICIENT_PLAYTIME,
                    expected=policy.min_playtime_minutes,
                    actual=playtime,
                )
            )

    return QualificationResult(
        appid=game.appid,
        qualified=not reasons,
        policy_version=policy_version,
        release_age_days=release_age_days,
        reasons=tuple(reasons),
    )


def parse_release_date(value: date | datetime | str | None) -> date | None:
    """Parse ISO and Steam English release dates without extra dependencies."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not value.strip():
        return None

    normalized = value.strip()
    for date_format in RELEASE_DATE_FORMATS:
        try:
            return datetime.strptime(normalized, date_format).date()
        except ValueError:
            continue
    return None


def _require_mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be a JSON object")
    return value


def _require_bool(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field} must be a boolean")
    return value


def _require_positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _require_non_negative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _require_non_empty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def _coerce_int(value: int | str | None) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_number(value: int | float | None) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value


def _serializable_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value
