import asyncio
from unittest.mock import AsyncMock, MagicMock

from bson import ObjectId

from app.repositories import analytics_repository, realtime_repository
from app.repositories.analytics_repository import _merge_count_metrics


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
    cursor = FakeCursor(
        [
            {
                "_id": 730,
                "appid": 730,
                "review_count": 10,
                "positive_reviews": 9,
                "negative_reviews": 1,
                "recommendation_rate": 0.9,
            },
            {
                "_id": 570,
                "appid": 570,
                "review_count": 20,
                "positive_reviews": 16,
                "negative_reviews": 4,
                "recommendation_rate": 0.8,
            },
        ]
    )
    collection = MagicMock(find=MagicMock(return_value=cursor))
    streamed = FakeCursor([])
    monkeypatch.setattr(
        analytics_repository,
        "get_collection",
        lambda name: collection if name == "game_metrics" else MagicMock(
            aggregate=MagicMock(return_value=streamed)
        ),
    )

    from app.repositories.analytics_repository import AnalyticsRepository

    result = asyncio.run(AnalyticsRepository().find_top_games(5))

    assert result == [
        {
            "appid": 730,
            "review_count": 10,
            "positive_reviews": 9,
            "negative_reviews": 1,
            "recommendation_rate": 0.9,
            "batch_review_count": 10,
            "stream_review_count": 0,
        },
        {
            "appid": 570,
            "review_count": 20,
            "positive_reviews": 16,
            "negative_reviews": 4,
            "recommendation_rate": 0.8,
            "batch_review_count": 20,
            "stream_review_count": 0,
        },
    ]


def test_hybrid_game_metrics_add_incremental_stream_reviews():
    result = _merge_count_metrics(
        [
            {
                "appid": 730,
                "game_name": "Counter-Strike 2",
                "review_count": 500,
                "positive_reviews": 400,
                "negative_reviews": 100,
                "recommendation_rate": 0.8,
                "serving_snapshot": "historical_v1",
            }
        ],
        [
            {
                "_id": 730,
                "game_name": "Counter-Strike 2",
                "review_count": 6,
                "positive_reviews": 4,
                "negative_reviews": 2,
            }
        ],
        "appid",
    )

    assert result == [
        {
            "appid": 730,
            "game_name": "Counter-Strike 2",
            "review_count": 506,
            "positive_reviews": 404,
            "negative_reviews": 102,
            "recommendation_rate": 404 / 506,
            "serving_snapshot": "batch_plus_streaming",
            "batch_review_count": 500,
            "stream_review_count": 6,
        }
    ]


def test_find_all_combines_batch_and_stream_metrics(monkeypatch):
    batch_cursor = FakeCursor(
        [
            {
                "_id": 730,
                "appid": 730,
                "game_name": "Counter-Strike 2",
                "review_count": 500,
                "positive_reviews": 400,
                "negative_reviews": 100,
                "recommendation_rate": 0.8,
            }
        ]
    )
    stream_cursor = FakeCursor(
        [
            {
                "_id": 730,
                "game_name": "Counter-Strike 2",
                "review_count": 6,
                "positive_reviews": 4,
                "negative_reviews": 2,
                "recommendation_rate": 4 / 6,
            }
        ]
    )
    batch_collection = MagicMock(find=MagicMock(return_value=batch_cursor))
    stream_collection = MagicMock(
        aggregate=MagicMock(return_value=stream_cursor)
    )
    monkeypatch.setattr(
        analytics_repository,
        "get_collection",
        lambda name: {
            "game_metrics": batch_collection,
            "recent_reviews": stream_collection,
        }[name],
    )

    result = asyncio.run(
        analytics_repository.AnalyticsRepository().find_all("game_metrics")
    )

    assert result[0]["review_count"] == 506
    assert result[0]["batch_review_count"] == 500
    assert result[0]["stream_review_count"] == 6
    assert result[0]["positive_reviews"] == 404
    assert result[0]["recommendation_rate"] == 404 / 506
    assert result[0]["serving_snapshot"] == "batch_plus_streaming"
    stream_collection.aggregate.assert_called_once()


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