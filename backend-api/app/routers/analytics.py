"""Read-only analytics endpoints serving the historical snapshot."""

from fastapi import APIRouter

from app.services.analytics_service import get_store

router = APIRouter(prefix="/api/analytics", tags=["analytics"])


@router.get("/games")
def games() -> dict:
    return {"data": get_store().games_analytics()}


@router.get("/genres")
def genres() -> dict:
    return {"data": get_store().genre_analytics()}


@router.get("/playtime")
def playtime() -> dict:
    return {"data": get_store().playtime_analytics()}


@router.get("/free-paid")
def free_paid() -> dict:
    return {"data": get_store().free_paid_analytics()}
