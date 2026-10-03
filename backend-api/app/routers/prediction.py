from fastapi import APIRouter, Depends
from starlette.concurrency import run_in_threadpool

from app.schemas.prediction import PredictionRequest, PredictionResponse
from app.services.prediction_service import PredictionService


router = APIRouter()
prediction_service = PredictionService()


def get_prediction_service() -> PredictionService:
    return prediction_service


@router.post("/api/ml/predict", response_model=PredictionResponse)
async def predict_review(
    request: PredictionRequest,
    service: PredictionService = Depends(get_prediction_service),
) -> PredictionResponse:
    return await run_in_threadpool(service.predict, request)
