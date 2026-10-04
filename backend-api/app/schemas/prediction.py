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


class WhatIfFeatureOptions(BaseModel):
    genres: list[GenreOrCategory] = Field(default_factory=list, max_length=32)
    categories: list[GenreOrCategory] = Field(default_factory=list, max_length=48)


class PredictionRequest(BaseModel):
    playtime_at_review: int | None = Field(default=None, ge=0)
    steam_purchase: bool | None = None
    received_for_free: bool | None = None
    is_free: bool | None = None
    price: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    genres: list[GenreOrCategory] = Field(default_factory=list, max_length=64)
    categories: list[GenreOrCategory] = Field(default_factory=list, max_length=128)
    platforms: PlatformFeatures = Field(default_factory=PlatformFeatures)
    what_if_options: WhatIfFeatureOptions = Field(
        default_factory=WhatIfFeatureOptions
    )


class FeatureContribution(BaseModel):
    attribute: str
    label: str
    value: float


class WhatIfFeatureEffect(BaseModel):
    feature_type: Literal["genre", "category"]
    value: str
    selected: bool
    probability_after_toggle: float = Field(ge=0, le=1)
    probability_delta: float


class PredictionExplanation(BaseModel):
    baseline_probability: float = Field(ge=0, le=1)
    baseline_description: str
    local_shap: list[FeatureContribution]
    global_importance: list[FeatureContribution]
    what_if_effects: list[WhatIfFeatureEffect] = Field(default_factory=list)


class PredictionResponse(BaseModel):
    model_name: str
    model_run_id: str | None
    prediction: Literal[0, 1]
    recommended: bool
    probability_positive: float = Field(ge=0, le=1)
    explanation: PredictionExplanation
