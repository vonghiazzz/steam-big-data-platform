from fastapi import APIRouter, Depends, Query

from app.schemas.response import DataResponse, TopGamesResponse
from app.services.analytics_service import AnalyticsService



router = APIRouter()


def get_analytics_service() -> AnalyticsService:
    return AnalyticsService()



@router.get("/api/analytics/games", response_model=DataResponse)
async def games(
    service: AnalyticsService = Depends(get_analytics_service),
) -> dict:
    return {"data": await service.get_games()}



@router.get("/api/analytics/games/top", response_model=TopGamesResponse)
async def top_games(
    limit: int = Query(default=10, ge=1, le=100),
    service: AnalyticsService = Depends(get_analytics_service),
) -> dict:
    return {"limit": limit, "data": await service.get_top_games(limit)}


@router.get("/api/analytics/genres", response_model=DataResponse)
async def genres(
    service: AnalyticsService = Depends(get_analytics_service),
) -> dict:
    return {"data": await service.get_metrics("genre_metrics")}


@router.get("/api/analytics/playtime", response_model=DataResponse)
async def playtime(
    service: AnalyticsService = Depends(get_analytics_service),
) -> dict:
    return {"data": await service.get_metrics("playtime_metrics")}


@router.get("/api/analytics/free-paid", response_model=DataResponse)
async def free_paid(
    service: AnalyticsService = Depends(get_analytics_service),
) -> dict:
    return {"data": await service.get_metrics("free_paid_metrics")}


@router.get("/api/analytics/platforms", response_model=DataResponse)
async def platforms(
    service: AnalyticsService = Depends(get_analytics_service),
) -> dict:
    return {"data": await service.get_metrics("platform_metrics")}


@router.get("/api/analytics/categories", response_model=DataResponse)
async def categories(
    service: AnalyticsService = Depends(get_analytics_service),
) -> dict:
    return {"data": await service.get_metrics("category_metrics")}


@router.get("/api/analytics/purchase", response_model=DataResponse)
async def purchase(
    service: AnalyticsService = Depends(get_analytics_service),
) -> dict:
    return {"data": await service.get_metrics("purchase_metrics")}