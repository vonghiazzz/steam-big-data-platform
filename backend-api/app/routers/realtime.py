"""Incremental review feed endpoints (local dev stand-in for the
streaming serving layer: latest sampled reviews, newest first)."""

from fastapi import APIRouter, Query

from app.services.analytics_service import get_store

router = APIRouter(prefix="/api/realtime", tags=["realtime"])


@router.get("/reviews")
def reviews(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
) -> dict:
    items, total = get_store().recent_reviews(page=page, page_size=page_size)
    return {"data": items, "total": total, "page": page, "page_size": page_size}
