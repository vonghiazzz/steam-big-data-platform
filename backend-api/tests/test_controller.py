import pytest
from fastapi.testclient import TestClient
from pymongo.errors import ServerSelectionTimeoutError

from app.main import app
from app.routers.analytics import get_analytics_service
from app.routers.health import get_health_service
from app.routers.prediction import get_prediction_service
from app.routers.realtime import get_realtime_service
from app.schemas.prediction import PredictionResponse
from app.services.exceptions import ModelUnavailableError, ResourceNotFoundError


class FakeAnalyticsService:
    async def get_games(self):
        return [{"appid": 730}]

    async def get_top_games(self, limit):
        return [{"appid": 730, "requested_limit": limit}]

    async def get_metrics(self, collection_name):
        return [{"collection": collection_name}]


class FakeRealtimeService:
    def __init__(self):
        self.review_args = None

    async def get_reviews(self, appid, page, page_size):
        self.review_args = (appid, page, page_size)
        return {
            "data": [{"appid": appid or 730}],
            "page": page,
            "page_size": page_size,
            "total": 1,
        }

    async def get_games(self):
        return [{"appid": 730, "review_count": 4}]

    async def get_game_metrics(self, appid):
        if appid == 999:
            raise ResourceNotFoundError("Game not found")
        return [{"appid": appid, "review_count": 4}]


class FakeHealthService:
    def __init__(self, failure=None):
        self.failure = failure

    async def check(self):
        if self.failure:
            raise self.failure


class FakePredictionService:
    def predict(self, request):
        if request.playtime_at_review == 999:
            raise ModelUnavailableError("Test model unavailable")
        return PredictionResponse(
            model_name="random_forest",
            model_run_id="test-run",
            prediction=1,
            recommended=True,
            probability_positive=0.82,
        )


@pytest.fixture
def client():
    realtime_service = FakeRealtimeService()
    app.dependency_overrides[get_analytics_service] = FakeAnalyticsService
    app.dependency_overrides[get_realtime_service] = lambda: realtime_service
    app.dependency_overrides[get_health_service] = FakeHealthService
    app.dependency_overrides[get_prediction_service] = FakePredictionService
    with TestClient(app) as test_client:
        yield test_client, realtime_service
    app.dependency_overrides.clear()


def test_health_returns_healthy(client):
    response = client[0].get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/api/analytics/games", [{"appid": 730}]),
        ("/api/analytics/genres", [{"collection": "genre_metrics"}]),
        ("/api/analytics/playtime", [{"collection": "playtime_metrics"}]),
        ("/api/analytics/free-paid", [{"collection": "free_paid_metrics"}]),
        ("/api/analytics/platforms", [{"collection": "platform_metrics"}]),
        ("/api/analytics/categories", [{"collection": "category_metrics"}]),
        ("/api/analytics/purchase", [{"collection": "purchase_metrics"}]),
        ("/api/realtime/games", [{"appid": 730, "review_count": 4}]),
    ],
)
def test_list_endpoints(client, path, expected):
    response = client[0].get(path)
    assert response.status_code == 200
    assert response.json()["data"] == expected


def test_top_games_and_review_pagination(client):
    test_client, realtime_service = client
    top_response = test_client.get("/api/analytics/games/top?limit=4")
    reviews_response = test_client.get(
        "/api/realtime/reviews?appid=730&page=2&page_size=10"
    )

    assert top_response.json() == {
        "limit": 4,
        "data": [{"appid": 730, "requested_limit": 4}],
    }
    assert reviews_response.json() == {
        "data": [{"appid": 730}],
        "page": 2,
        "page_size": 10,
        "total": 1,
    }
    assert realtime_service.review_args == (730, 2, 10)


def test_realtime_game_detail_and_not_found(client):
    test_client = client[0]
    assert test_client.get("/api/realtime/games/730").json() == {
        "data": [{"appid": 730, "review_count": 4}]
    }
    response = test_client.get("/api/realtime/games/999")
    assert response.status_code == 404
    assert response.json() == {
        "error": {"code": "RESOURCE_NOT_FOUND", "message": "Game not found"}
    }


@pytest.mark.parametrize(
    "path",
    [
        "/api/analytics/games/top?limit=101",
        "/api/realtime/reviews?page=0",
        "/api/realtime/reviews?page_size=101",
        "/api/realtime/reviews?appid=0",
        "/api/realtime/games/0",
    ],
)
def test_invalid_parameters_return_consistent_error(client, path):
    response = client[0].get(path)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_PARAMETER"


def test_database_failure_is_sanitized(client):
    app.dependency_overrides[get_health_service] = lambda: FakeHealthService(
        ServerSelectionTimeoutError("private connection detail")
    )
    response = client[0].get("/api/health")

    assert response.status_code == 500
    assert response.json() == {
        "error": {
            "code": "DATABASE_ERROR",
            "message": "Database operation failed",
        }
    }
    assert "private connection detail" not in response.text


def test_ml_prediction_endpoint_returns_model_result(client):
    response = client[0].post(
        "/api/ml/predict",
        json={
            "playtime_at_review": 600,
            "steam_purchase": True,
            "received_for_free": False,
            "is_free": False,
            "price": 19.99,
            "genres": ["Action", "RPG"],
            "categories": ["Single-player"],
            "platforms": {"windows": True, "mac": False, "linux": None},
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "model_name": "random_forest",
        "model_run_id": "test-run",
        "prediction": 1,
        "recommended": True,
        "probability_positive": 0.82,
    }


def test_ml_prediction_endpoint_rejects_negative_features(client):
    response = client[0].post(
        "/api/ml/predict",
        json={"playtime_at_review": -1},
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_PARAMETER"


def test_ml_prediction_endpoint_reports_unavailable_model(client):
    response = client[0].post(
        "/api/ml/predict",
        json={"playtime_at_review": 999},
    )

    assert response.status_code == 503
    assert response.json() == {
        "error": {
            "code": "ML_MODEL_UNAVAILABLE",
            "message": "Test model unavailable",
        }
    }


@pytest.mark.parametrize(
    "origin",
    [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
)
def test_cors_allows_local_frontend_get_and_prediction_post(client, origin):
    response = client[0].options(
        "/api/ml/predict",
        headers={
            "Origin": origin,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == origin
    assert "POST" in response.headers["access-control-allow-methods"]