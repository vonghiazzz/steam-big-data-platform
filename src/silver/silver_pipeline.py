from pyspark.sql import SparkSession

from common.config import *


from silver.clean_games import transform_games
from silver.clean_reviews import transform_reviews



spark = SparkSession.builder \
.appName(
"Silver Pipeline"
)\
.getOrCreate()



games_df = spark.read.json(
    BRONZE_GAMES_PATH
)


reviews_df = spark.read.json(
    BRONZE_REVIEWS_PATH
)



games_silver = transform_games(
    games_df
)


reviews_silver = transform_reviews(
    reviews_df
)



games_silver.write \
.mode("overwrite") \
.parquet(
    SILVER_GAMES_PATH
)


reviews_silver.write \
.mode("overwrite") \
.parquet(
    SILVER_REVIEWS_PATH
)


spark.stop()