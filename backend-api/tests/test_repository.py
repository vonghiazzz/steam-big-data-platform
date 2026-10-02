import asyncio
from unittest.mock import AsyncMock, MagicMock

from bson import ObjectId

from app.repositories import analytics_repository, realtime_repository


class FakeCursor:
    def __init__(self, documents):
        self.documents = documents
        self.sort_args = None
        self.skip_value = None
        self.limit_value = None

    def sort(self, *args):
        self.sort_args = args
        return self

    def skip(self, value):
        self.skip_value = value
        return self

    def limit(self, value):
        self.limit_value = value
        return self

    def __aiter__(self):
        async def iterate():
            for document in self.documents:
                yield document

        return iterate()


def test_find_all_hides_mongo_id_and_converts_nested_object_ids(monkeypatch):
    mongo_id = ObjectId()
    cursor = FakeCursor([{"_id": mongo_id, "appid": 730, "meta": {"id": mongo_id}}])
    collection = MagicMock(find=MagicMock(return_value=cursor))
    monkeypatch.setattr(
        analytics_repository, "get_collection", lambda name: collection
    )

    from app.repositories.analytics_repository import AnalyticsRepository

    result = asyncio.run(AnalyticsRepository().find_all("game_metrics"))

    assert result == [{"appid": 730, "meta": {"id": str(mongo_id)}}]
    collection.find.assert_called_once_with({})


def test_find_top_games_sorts_and_limits(monkeypatch):
    cursor = FakeCursor([{"_id": 730, "appid": 730, "recommendation_rate": 0.9}])
    collection = MagicMock(find=MagicMock(return_value=cursor))
    monkeypatch.setattr(
        analytics_repository, "get_collection", lambda name: collection
    )

    from app.repositories.analytics_repository import AnalyticsRepository

    result = asyncio.run(AnalyticsRepository().find_top_games(5))

    assert result == [{"appid": 730, "recommendation_rate": 0.9}]
    assert cursor.sort_args == (
        [("recommendation_rate", -1), ("review_count", -1), ("appid", 1)],
    )
    assert cursor.limit_value == 5


def test_find_reviews_filters_and_applies_pagination(monkeypatch):
    cursor = FakeCursor([{"_id": "r1", "appid": 730}])
    collection = MagicMock(
        find=MagicMock(return_value=cursor),
        count_documents=AsyncMock(return_value=31),
    )
    monkeypatch.setattr(realtime_repository, "get_collection", lambda name: collection)

    from app.repositories.realtime_repository import RealtimeRepository

    result, total = asyncio.run(RealtimeRepository().find_reviews(730, 3, 10))

    assert result == [{"appid": 730}]
    assert total == 31
    collection.find.assert_called_once_with({"appid": 730})
    collection.count_documents.assert_awaited_once_with({"appid": 730})
    assert cursor.skip_value == 20
    assert cursor.limit_value == 10


def test_find_game_metrics_filters_appid_and_sorts(monkeypatch):
    cursor = FakeCursor([{"_id": "730:window", "appid": 730}])
    collection = MagicMock(find=MagicMock(return_value=cursor))
    monkeypatch.setattr(realtime_repository, "get_collection", lambda name: collection)

    from app.repositories.realtime_repository import RealtimeRepository

    result = asyncio.run(RealtimeRepository().find_game_metrics(730))

    assert result == [{"appid": 730}]
    collection.find.assert_called_once_with({"appid": 730})
    assert cursor.sort_args == ([("window_start", -1), ("appid", 1)],)