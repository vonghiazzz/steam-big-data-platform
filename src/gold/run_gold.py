"""Build validated Steam Gold Base from the canonical Silver datasets."""

from pyspark.sql import SparkSession

from common.config import HDFS_GOLD_BASE, HDFS_SILVER_GAMES, HDFS_SILVER_REVIEWS
from gold.gold_join import create_gold_dataset, validate_gold_dataset


def main() -> None:
    spark = SparkSession.builder.appName("Steam Gold Base Pipeline").getOrCreate()

    try:
        reviews_df = spark.read.parquet(HDFS_SILVER_REVIEWS)
        games_df = spark.read.parquet(HDFS_SILVER_GAMES)

        gold_df = create_gold_dataset(reviews_df, games_df)
        validate_gold_dataset(reviews_df, games_df, gold_df)

        gold_df.write.mode("overwrite").parquet(HDFS_GOLD_BASE)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
