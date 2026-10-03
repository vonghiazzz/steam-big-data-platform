from typing import Literal

from pydantic import BaseModel, Field, StringConstraints
from typing_extensions import Annotated


GenreOrCategory = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=120),
]


class PlatformFeatures(BaseModel):
    windows: bool | None = None
    mac: bool | None = None
    linux: bool | None = None


class PredictionRequest(BaseModel):
    playtime_at_review: int | None = Field(default=None, ge=0)
    steam_purchase: bool | None = None
    received_for_free: bool | None = None
    is_free: bool | None = None
    price: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    genres: list[GenreOrCategory] = Field(default_factory=list, max_length=64)
    categories: list[GenreOrCategory] = Field(default_factory=list, max_length=128)
    platforms: PlatformFeatures = Field(default_factory=PlatformFeatures)


class PredictionResponse(BaseModel):
    model_name: str
    model_run_id: str | None
    prediction: Literal[0, 1]
    recommended: bool
    probability_positive: float = Field(ge=0, le=1)
