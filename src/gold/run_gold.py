from pyspark.sql import SparkSession

from gold.gold_join import (
    create_gold_dataset,
    validate_join
)



def main():

    spark = (
        SparkSession.builder
        .appName(
            "Steam Gold Dataset"
        )
        .getOrCreate()
    )


    reviews_df = spark.read.parquet(
        "hdfs://namenode:8020/steam/silver/reviews"
    )


    games_df = spark.read.parquet(
        "hdfs://namenode:8020/steam/silver/games"
    )


    # create gold
    gold_df = create_gold_dataset(
        reviews_df,
        games_df
    )


    # validation
    validate_join(
        reviews_df,
        games_df,
        gold_df
    )


    # write gold
    gold_df.write \
        .mode("overwrite") \
        .parquet(
            "hdfs://namenode:8020/steam/gold/base"
        )


    print(
        "Gold dataset written successfully"
    )


    spark.stop()



if __name__ == "__main__":
    main()