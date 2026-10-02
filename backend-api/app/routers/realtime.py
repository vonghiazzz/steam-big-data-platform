from fastapi import APIRouter, Depends, Path, Query

from app.schemas.response import DataResponse, ReviewPageResponse
from app.services.realtime_service import RealtimeService


router = APIRouter()


def get_realtime_service() -> RealtimeService:
    return RealtimeService()


@router.get("/api/realtime/reviews", response_model=ReviewPageResponse)
async def recent_reviews(
    appid: int | None = Query(default=None, ge=1),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    service: RealtimeService = Depends(get_realtime_service),
) -> dict:
    return await service.get_reviews(appid, page, page_size)


@router.get("/api/realtime/games", response_model=DataResponse)
async def realtime_games(
    service: RealtimeService = Depends(get_realtime_service),
) -> dict:
    return {"data": await service.get_games()}


@router.get("/api/realtime/games/{appid}", response_model=DataResponse)
async def realtime_game_detail(
    appid: int = Path(ge=1),
    service: RealtimeService = Depends(get_realtime_service),
) -> dict:
    return {"data": await service.get_game_metrics(appid)}