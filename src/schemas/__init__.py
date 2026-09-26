"""PySpark schemas used by the Steam data platform."""

from .steam_bronze_schema import (
    STEAM_GAMES_BRONZE_SCHEMA,
    STEAM_REVIEWS_BRONZE_SCHEMA,
)

__all__ = ["STEAM_GAMES_BRONZE_SCHEMA", "STEAM_REVIEWS_BRONZE_SCHEMA"]
