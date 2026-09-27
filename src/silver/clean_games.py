from pyspark.sql import functions as F


def transform_games(games_df):

    games_df = (
        games_df

        # =========================
        # CLEAN BOOLEAN
        # =========================
        .withColumn(
            "is_free",
            F.coalesce(
                F.col("is_free"),
                F.lit(False)
            )
        )


        # =========================
        # PRICE
        # =========================
        # Steam bronze does not contain price
        # Free games = 0
        # Paid games = unknown(null)
        .withColumn(
            "price",
            F.when(
                F.col("is_free") == True,
                F.lit(0.0)
            )
            .otherwise(
                F.lit(None).cast("double")
            )
        )


        # =========================
        # NORMALIZE GAME NAME
        # =========================
        .withColumn(
            "game_name_clean",
            F.lower(
                F.trim(
                    F.col("name")
                )
            )
        )


        # =========================
        # NORMALIZE PRIMARY GENRE
        # =========================
        .withColumn(
            "primary_genre_clean",
            F.when(
                F.col("primary_genre").isNull(),
                F.lit("unknown")
            )
            .otherwise(
                F.lower(
                    F.trim(
                        F.col("primary_genre")
                    )
                )
            )
        )


        # =========================
        # RELEASE YEAR
        # =========================
        .withColumn(
            "release_year",
            F.regexp_extract(
                F.col("release_date"),
                r"(\d{4})",
                1
            )
            .cast("integer")
        )


        # =========================
        # SELECT SILVER DATASET
        # =========================
        .select(
            "appid",

            F.col("name")
            .alias("game_name"),

            "game_name_clean",

            "genres",

            "primary_genre",

            "primary_genre_clean",

            "is_free",

            "price",

            "release_date",

            "release_year",

            "total_reviews",

            "total_positive",

            "total_negative",

            "overall_positive_rate",

            "selection_bucket"
        )
    )


    return games_df