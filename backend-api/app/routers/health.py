from fastapi import APIRouter, Depends

from app.schemas.response import HealthResponse
from app.services.health_service import HealthService


router = APIRouter()


def get_health_service() -> HealthService:
    return HealthService()


@router.get(
    "/api/health"
)
async def health(
    service: HealthService = Depends(get_health_service),
) -> dict[str, str]:
    await service.check()
    return {"status": "healthy"}