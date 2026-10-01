from typing import Any

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str


class DataResponse(BaseModel):
    data: list[dict[str, Any]]


class TopGamesResponse(DataResponse):
    limit: int


class ReviewPageResponse(DataResponse):
    page: int
    page_size: int
    total: int


class ErrorDetail(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    error: ErrorDetail