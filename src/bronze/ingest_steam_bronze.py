from pyspark.sql import SparkSession

from schemas.steam_bronze_schema import (
    STEAM_GAMES_BRONZE_SCHEMA,
    STEAM_REVIEWS_BRONZE_SCHEMA
)


from common.config import (
    BRONZE_GAMES_PATH,
    BRONZE_REVIEWS_PATH
)


spark = (
    SparkSession.builder
    .appName(
        "Steam Bronze Ingestion"
    )
    .getOrCreate()
)


games_df = (
    spark.read
    .schema(
        STEAM_GAMES_BRONZE_SCHEMA
    )
    .json(
        BRONZE_GAMES_PATH
    )
)


reviews_df = (
    spark.read
    .schema(
        STEAM_REVIEWS_BRONZE_SCHEMA
    )
    .json(
        BRONZE_REVIEWS_PATH
    )
)


print("=== GAMES ===")

games_df.printSchema()

print(
    "Games count:",
    games_df.count()
)


print("=== REVIEWS ===")

reviews_df.printSchema()

print(
    "Reviews count:",
    reviews_df.count()
)


games_df.write.mode(
    "overwrite"
).parquet(
    "/steam/bronze/parquet/games"
)


reviews_df.write.mode(
    "overwrite"
).parquet(
    "/steam/bronze/parquet/reviews"
)


spark.stop()