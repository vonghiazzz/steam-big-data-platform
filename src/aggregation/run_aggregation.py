from pyspark.sql import SparkSession

from aggregation.mapper import mapper
from aggregation.reducer import reducer



spark = SparkSession.builder \
    .appName("GameReviewAggregation") \
    .getOrCreate()



gold_df = spark.read.parquet(
    "hdfs://namenode:8020/steam/gold/base"
)



rdd = gold_df.rdd



mapped = rdd.map(mapper)



grouped = mapped.groupByKey()



result = grouped.map(
    lambda x: reducer(
        x[0],
        x[1]
    )
)



result_df = spark.createDataFrame(
    result
)



result_df.write \
    .mode("overwrite") \
    .parquet(
        "hdfs://namenode:8020/steam/gold/game_aggregation"
    )



print("Aggregation completed")