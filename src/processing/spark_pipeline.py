"""
Steam Big Data Platform
PySpark Bronze -> Silver -> Gold Pipeline

Task:
- Read raw Steam game/review JSON
- Clean data
- Transform features
- Write Silver Parquet
- Join data
- Write Gold analytics dataset
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================
# PATH CONFIG
# =========================

from common.config import (
    LANDING_GAMES_RAW_PATH,
    LANDING_REVIEWS_ROOT,
    HDFS_SILVER_ROOT,
    HDFS_GOLD_ROOT,
)

SILVER_GAMES_PATH = (
    f"{HDFS_SILVER_ROOT}/games"
)

SILVER_REVIEWS_PATH = (
    f"{HDFS_SILVER_ROOT}/reviews"
)

GOLD_PATH = (
    f"{HDFS_GOLD_ROOT}/steam_analysis"
)


# =========================
# SPARK SESSION
# =========================

def create_spark():

    spark = (
        SparkSession.builder
        .appName("Steam-BigData-Pipeline")
        .master("local[*]")
        .config(
            "spark.sql.shuffle.partitions",
            "8"
        )
        .getOrCreate()
    )

    return spark


# =========================
# INGESTION
# =========================

def read_raw_data(spark):

    print("Reading Bronze/Landing data...")


    games_df = (
        spark.read
        .json(
            str(LANDING_GAMES_RAW_PATH)
        )
    )


    reviews_df = (
        spark.read
        .json(
            str(LANDING_REVIEWS_ROOT)
        )
    )


    print(
        "Games:",
        games_df.count()
    )

    print(
        "Reviews:",
        reviews_df.count()
    )


    return games_df, reviews_df


# =========================
# CLEANING
# =========================

def clean_games(df):

    print("Cleaning games...")


    df = (
        df
        .dropDuplicates(
            ["appid"]
        )
        .filter(
            F.col("appid").isNotNull()
        )
        .filter(
            F.col("name").isNotNull()
        )
    )


    return df



def clean_reviews(df):

    print("Cleaning reviews...")


    df = (
        df
        .dropDuplicates(
            ["recommendationid"]
        )
        .filter(
            F.col("appid").isNotNull()
        )
    )


    return df



# =========================
# TRANSFORMATION
# =========================

def transform_games(df):

    print("Transform games...")


    df = (
        df
        .withColumn(
            "free_game",
            F.when(
                F.col("is_free") == True,
                1
            )
            .otherwise(0)
        )
    )


    return df



def transform_reviews(df):

    print("Transform reviews...")


    df = (
        df
        .withColumn(
            "playtime_hours",
            F.round(
                F.col("playtime_forever") / 60,
                2
            )
        )
    )


    return df



# =========================
# SILVER LAYER
# =========================

def write_silver(
        games_df,
        reviews_df
):

    print("Writing Silver layer...")


    (
        games_df
        .write
        .mode("overwrite")
        .parquet(
            SILVER_GAMES_PATH
        )
    )


    (
        reviews_df
        .write
        .mode("overwrite")
        .parquet(
            SILVER_REVIEWS_PATH
        )
    )



# =========================
# GOLD LAYER
# =========================

def create_gold(
        games_df,
        reviews_df
):

    print("Creating Gold dataset...")


    gold_df = (
        reviews_df
        .join(
            games_df,
            on="appid",
            how="inner"
        )
    )


    return gold_df



def write_gold(df):

    print("Writing Gold layer...")


    (
        df
        .write
        .mode("overwrite")
        .parquet(
            GOLD_PATH
        )
    )



# =========================
# VALIDATION
# =========================

def validate(df):

    print("===================")
    print("DATA VALIDATION")
    print("===================")


    print(
        "Total rows:",
        df.count()
    )


    print(
        "Columns:"
    )

    df.printSchema()


    print(
        "Sample:"
    )

    df.show(
        5,
        truncate=False
    )



# =========================
# MAIN PIPELINE
# =========================

def main():

    spark = create_spark()


    # Bronze read
    games_df, reviews_df = read_raw_data(
        spark
    )


    # Cleaning
    games_df = clean_games(
        games_df
    )

    reviews_df = clean_reviews(
        reviews_df
    )


    # Feature engineering
    games_df = transform_games(
        games_df
    )

    reviews_df = transform_reviews(
        reviews_df
    )


    # Silver
    write_silver(
        games_df,
        reviews_df
    )


    # Gold
    gold_df = create_gold(
        games_df,
        reviews_df
    )


    write_gold(
        gold_df
    )


    # Check result
    validate(
        gold_df
    )


    spark.stop()



if __name__ == "__main__":
    main()