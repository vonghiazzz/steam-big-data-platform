from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
    .appName("Bronze Quality Profiling")
    .getOrCreate()
)


# =========================
# READ BRONZE
# =========================

games_df = (
    spark.read
    .schema(STEAM_GAMES_BRONZE_SCHEMA)
    .json(BRONZE_GAMES_PATH)
)


reviews_df = (
    spark.read
    .schema(STEAM_REVIEWS_BRONZE_SCHEMA)
    .json(BRONZE_REVIEWS_PATH)
)



# =========================
# 1. COUNT CHECK
# =========================

print("===== DATA COUNT =====")

print(
    "Games:",
    games_df.count()
)

print(
    "Reviews:",
    reviews_df.count()
)



# =========================
# 2. SCHEMA CHECK
# =========================

print("\n===== GAMES SCHEMA =====")

games_df.printSchema()


print("\n===== REVIEWS SCHEMA =====")

reviews_df.printSchema()



# =========================
# 3. NULL CHECK
# =========================

print("\n===== GAMES NULL COUNT =====")


games_df.select(
    [
        F.sum(
            F.when(
                F.col(c).isNull(),
                1
            )
            .otherwise(0)
        )
        .alias(c)

        for c in games_df.columns
    ]
).show()



print("\n===== REVIEWS NULL COUNT =====")


reviews_df.select(
    [
        F.sum(
            F.when(
                F.col(c).isNull(),
                1
            )
            .otherwise(0)
        )
        .alias(c)

        for c in reviews_df.columns
    ]
).show()

print("\n===== REVIEW STRUCT NULL CHECK =====")

reviews_df.select(
    F.count(
        F.when(
            F.col("review.recommendationid").isNull(),
            1
        )
    ).alias("missing_recommendationid"),

    F.count(
        F.when(
            F.col("review.author").isNull(),
            1
        )
    ).alias("missing_author")

).show()

# =========================
# 4. DUPLICATE CHECK
# =========================

print("\n===== DUPLICATES =====")


print(
    "Duplicate games appid:"
)


games_df.groupBy(
    "appid"
)\
.count()\
.filter(
    F.col("count") > 1
)\
.show()



print(
    "Duplicate reviews recommendationid:"
)

reviews_df.groupBy(
    "review.recommendationid"
).count() \
.filter(
    F.col("count") > 1
).show()



# =========================
# 5. INVALID KEY CHECK
# =========================

print("\n===== INVALID KEYS =====")


invalid_games = (
    games_df
    .filter(
        (F.col("appid").isNull())
        |
        (F.col("appid") <= 0)
    )
    .count()
)


print(
    "Invalid games appid:",
    invalid_games
)



invalid_reviews = (
    reviews_df
    .filter(
        (F.col("appid").isNull())
        |
        (F.col("appid") <= 0)
    )
    .count()
)


print(
    "Invalid reviews appid:",
    invalid_reviews
)



invalid_recommendation = (
    reviews_df
    .filter(
        F.col("recommendationid").isNull()
    )
    .count()
)


print(
    "Invalid recommendationid:",
    invalid_recommendation
)



# =========================
# 6. PLAYTIME OUTLIER
# =========================

print("\n===== PLAYTIME OUTLIERS =====")


reviews_df.select(
    F.col("author.playtime_forever")
)\
.summary(
    "count",
    "min",
    "max",
    "mean"
)\
.show()



# =========================
# 7. IQR OUTLIER DETECTION
# =========================

print("\n===== PLAYTIME IQR ANALYSIS =====")


quantiles = reviews_df.approxQuantile(
    "author.playtime_forever",
    [0.25, 0.75],
    0.01
)


if len(quantiles) == 2:

    q1 = quantiles[0]
    q3 = quantiles[1]

    iqr = q3 - q1

    upper_bound = q3 + 1.5 * iqr


    print("Q1:", q1)
    print("Q3:", q3)
    print("IQR:", iqr)
    print("Upper Bound:", upper_bound)


    outlier_count = (
        reviews_df
        .filter(
            F.col("author.playtime_forever") > upper_bound
        )
        .count()
    )


    print(
        "Playtime outliers:",
        outlier_count
    )
print("\n===== REVIEWS PER GAME =====")

reviews_df.groupBy(
    "appid"
).count().orderBy(
    F.desc("count")
).show(10)

print("\n===== INVALID APPID TYPE =====")

games_df.filter(
    F.col("appid") <= 0
).count()

reviews_df.filter(
    F.col("appid") <= 0
).count()

print("\n===== INVALID PLAYTIME =====")

reviews_df.filter(
    F.col(
        "review.author.playtime_forever"
    ) < 0
).count()

reviews_df.select(
    "review.recommended"
).distinct().show()

spark.stop()