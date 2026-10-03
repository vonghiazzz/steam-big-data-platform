import pytest

from app.schemas.prediction import PredictionRequest
from app.services.exceptions import ModelUnavailableError
from app.services.prediction_service import PredictionService


def test_prediction_requires_an_explicit_model_path(monkeypatch):
    monkeypatch.setattr(
        "app.services.prediction_service.settings.ML_MODEL_PATH",
        "",
    )

    with pytest.raises(ModelUnavailableError, match="Set ML_MODEL_PATH"):
        PredictionService().predict(PredictionRequest())
