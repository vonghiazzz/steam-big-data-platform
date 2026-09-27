from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    ArrayType,
)

STEAM_GAMES_BRONZE_SCHEMA = StructType([


    StructField(
        "appid",
        IntegerType(),
        True
    ),


    StructField(
        "data",
        StructType([


            StructField(
                "name",
                StringType(),
                True
            ),


            StructField(
                "genres",
                ArrayType(
                    StructType([

                        StructField(
                            "id",
                            StringType(),
                            True
                        ),

                        StructField(
                            "description",
                            StringType(),
                            True
                        )

                    ])
                ),
                True
            ),


            StructField(
                "is_free",
                BooleanType(),
                True
            ),


            StructField(
                "release_date",
                StringType(),
                True
            )

        ]),
        True
    )
])

STEAM_REVIEWS_BRONZE_SCHEMA = StructType([


StructField(
    "appid",
    IntegerType(),
    True
),


StructField(
    "review",
    StructType([


        StructField(
            "recommendationid",
            StringType(),
            True
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
            "voted_up",
            BooleanType(),
            True
        ),


        StructField(
            "review",
            StringType(),
            True
        )

    ]),
    True
)

])