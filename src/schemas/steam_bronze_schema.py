from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    ArrayType,
    DoubleType,
    LongType,
    TimestampType,
)


STEAM_GAMES_BRONZE_SCHEMA = StructType([

    StructField(
        "appid",
        IntegerType(),
        False
    ),

    StructField(
        "name",
        StringType(),
        True
    ),

    StructField(
        "type",
        StringType(),
        True
    ),

    StructField(
        "required_age",
        IntegerType(),
        True
    ),

    StructField(
        "is_free",
        BooleanType(),
        True
    ),

    StructField(
        "developers",
        ArrayType(StringType()),
        True
    ),

    StructField(
        "publishers",
        ArrayType(StringType()),
        True
    ),

    StructField(
        "genres",
        ArrayType(StringType()),
        True
    ),

    StructField(
        "categories",
        ArrayType(StringType()),
        True
    ),

    StructField(
        "release_date",
        StringType(),
        True
    ),

    StructField(
        "coming_soon",
        BooleanType(),
        True
    ),

    StructField(
        "price_overview",
        StructType([

            StructField(
                "currency",
                StringType(),
                True
            ),

            StructField(
                "initial",
                IntegerType(),
                True
            ),

            StructField(
                "final",
                IntegerType(),
                True
            ),

            StructField(
                "discount_percent",
                IntegerType(),
                True
            ),

            StructField(
                "final_formatted",
                StringType(),
                True
            )

        ]),
        True
    ),

    StructField(
        "collected_at",
        TimestampType(),
        True
    )

])

STEAM_REVIEWS_BRONZE_SCHEMA = StructType([


    StructField(
        "recommendationid",
        StringType(),
        False
    ),


    StructField(
        "appid",
        IntegerType(),
        False
    ),


    StructField(
        "author",
        StructType([

            StructField(
                "steamid",
                StringType(),
                True
            ),

            StructField(
                "num_games_owned",
                IntegerType(),
                True
            ),

            StructField(
                "num_reviews",
                IntegerType(),
                True
            ),

            StructField(
                "playtime_forever",
                IntegerType(),
                True
            ),

            StructField(
                "playtime_last_two_weeks",
                IntegerType(),
                True
            )

        ]),
        True
    ),


    StructField(
        "language",
        StringType(),
        True
    ),


    StructField(
        "review",
        StringType(),
        True
    ),


    StructField(
        "timestamp_created",
        LongType(),
        True
    ),


    StructField(
        "timestamp_updated",
        LongType(),
        True
    ),


    StructField(
        "voted_up",
        BooleanType(),
        True
    ),


    StructField(
        "votes_up",
        IntegerType(),
        True
    ),


    StructField(
        "votes_funny",
        IntegerType(),
        True
    ),


    StructField(
        "weighted_vote_score",
        DoubleType(),
        True
    ),


    StructField(
        "comment_count",
        IntegerType(),
        True
    ),


    StructField(
        "steam_purchase",
        BooleanType(),
        True
    ),


    StructField(
        "received_for_free",
        BooleanType(),
        True
    ),


    StructField(
        "written_during_early_access",
        BooleanType(),
        True
    ),


    StructField(
        "collected_at",
        TimestampType(),
        True
    )

])