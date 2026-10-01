import asyncio
from unittest.mock import AsyncMock

import pytest

from app.services.analytics_service import AnalyticsService
from app.services.health_service import HealthService
from app.services.exceptions import ResourceNotFoundError
from app.services.realtime_service import RealtimeService


def test_analytics_service_delegates_collection_and_limit():
    repository = AsyncMock()
    repository.find_all.return_value = [{"appid": 730}]
    repository.find_top_games.return_value = [{"appid": 730}]
    service = AnalyticsService(repository)

    assert asyncio.run(service.get_games()) == [{"appid": 730}]
    assert asyncio.run(service.get_metrics("genre_metrics")) == [{"appid": 730}]
    assert asyncio.run(service.get_top_games(7)) == [{"appid": 730}]
    repository.find_all.assert_any_await("game_metrics")
    repository.find_all.assert_any_await("genre_metrics")
    repository.find_top_games.assert_awaited_once_with(7)


def test_realtime_service_returns_review_pagination_metadata():
    repository = AsyncMock()
    repository.find_reviews.return_value = ([{"appid": 730}], 21)
    service = RealtimeService(repository)

    result = asyncio.run(service.get_reviews(730, 2, 10))

    assert result == {
        "data": [{"appid": 730}],
        "page": 2,
        "page_size": 10,
        "total": 21,
    }
    repository.find_reviews.assert_awaited_once_with(730, 2, 10)


def test_realtime_service_raises_not_found_for_unknown_game():
    repository = AsyncMock()
    repository.find_game_metrics.return_value = []

    with pytest.raises(ResourceNotFoundError, match="Game not found"):
        asyncio.run(RealtimeService(repository).get_game_metrics(999))


def test_health_service_pings_repository():
    repository = AsyncMock()

    asyncio.run(HealthService(repository).check())

    repository.ping.assert_awaited_once_with()