from pyspark.sql import functions as F


def transform_reviews(reviews_df):

    reviews_df = (
        reviews_df

        # Flatten nested review fields
        .withColumn(
            "recommendationid",
            F.col("review.recommendationid")
        )

        .withColumn(
            "voted_up",
            F.col("review.voted_up")
        )

        .withColumn(
            "steam_purchase",
            F.col("review.steam_purchase")
        )

        .withColumn(
            "received_for_free",
            F.col("review.received_for_free")
        )


        # playtime minute -> hour
        # use playtime_at_review because it reflects behavior before review
        .withColumn(
            "playtime_hours",
            (
                F.col(
                    "review.author.playtime_at_review"
                ) / 60
            )
            .cast("double")
        )


        # recommendation label for ML target
        .withColumn(
            "recommendation_label",
            F.when(
                F.col("review.voted_up") == True,
                1
            )
            .otherwise(0)
            .cast("integer")
        )


        # normalize language
        .withColumn(
            "language_clean",
            F.when(
                F.col("review.language").isNull(),
                F.lit("unknown")
            )
            .otherwise(
                F.lower(
                    F.trim(
                        F.col("review.language")
                    )
                )
            )
        )

        .withColumn(
            "review_year",
            F.from_unixtime(
                F.col("review.timestamp_created")
            ).substr(1,4)
        )


        # Select Silver columns
        .select(
            "appid",
            "game_name",
            "ingested_at",

            "recommendationid",

            "voted_up",
            "recommendation_label",

            "steam_purchase",
            "received_for_free",

            "playtime_hours",

            "language_clean",
            "review_year"
        )
    )

    return reviews_df