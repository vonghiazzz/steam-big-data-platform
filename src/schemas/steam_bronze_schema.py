"""Explicit schemas for the raw Steam Bronze JSONL wrappers."""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


PLATFORM_SCHEMA = StructType(
    [
        StructField("windows", BooleanType(), True),
        StructField("mac", BooleanType(), True),
        StructField("linux", BooleanType(), True),
    ]
)

CATEGORY_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("description", StringType(), True),
    ]
)

GENRE_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("description", StringType(), True),
    ]
)

PRICE_OVERVIEW_SCHEMA = StructType(
    [
        StructField("currency", StringType(), True),
        StructField("initial", LongType(), True),
        StructField("final", LongType(), True),
        StructField("discount_percent", IntegerType(), True),
        StructField("initial_formatted", StringType(), True),
        StructField("final_formatted", StringType(), True),
    ]
)

PACKAGE_SUB_SCHEMA = StructType(
    [
        StructField("packageid", IntegerType(), True),
        StructField("option_text", StringType(), True),
        StructField("option_description", StringType(), True),
        # Steam returns this field as a JSON string in the canonical records.
        StructField("can_get_free_license", StringType(), True),
        StructField("is_free_license", BooleanType(), True),
        StructField("price_in_cents_with_discount", LongType(), True),
    ]
)

PACKAGE_GROUP_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("title", StringType(), True),
        # Steam returns this field as "true"/"false" strings.
        StructField("is_recurring_subscription", StringType(), True),
        StructField("subs", ArrayType(PACKAGE_SUB_SCHEMA), True),
    ]
)

RELEASE_DATE_SCHEMA = StructType(
    [
        StructField("coming_soon", BooleanType(), True),
        StructField("date", StringType(), True),
    ]
)

GAME_DATA_SCHEMA = StructType(
    [
        StructField("type", StringType(), True),
        StructField("name", StringType(), True),
        StructField("steam_appid", IntegerType(), True),
        # Steam returns both JSON strings and numbers for required_age.
        StructField("required_age", StringType(), True),
        StructField("is_free", BooleanType(), True),
        StructField("developers", ArrayType(StringType()), True),
        StructField("publishers", ArrayType(StringType()), True),
        StructField("platforms", PLATFORM_SCHEMA, True),
        StructField("categories", ArrayType(CATEGORY_SCHEMA), True),
        StructField("genres", ArrayType(GENRE_SCHEMA), True),
        StructField("price_overview", PRICE_OVERVIEW_SCHEMA, True),
        StructField("package_groups", ArrayType(PACKAGE_GROUP_SCHEMA), True),
        StructField("release_date", RELEASE_DATE_SCHEMA, True),
    ]
)

STEAM_GAMES_BRONZE_SCHEMA = StructType(
    [
        StructField("appid", IntegerType(), True),
        StructField("collected_at", StringType(), True),
        StructField("source", StringType(), True),
        StructField("success", BooleanType(), True),
        StructField("data", GAME_DATA_SCHEMA, True),
    ]
)

REVIEW_AUTHOR_SCHEMA = StructType(
    [
        StructField("steamid", StringType(), True),
        StructField("num_games_owned", IntegerType(), True),
        StructField("num_reviews", IntegerType(), True),
        StructField("playtime_forever", IntegerType(), True),
        StructField("playtime_last_two_weeks", IntegerType(), True),
        StructField("playtime_at_review", IntegerType(), True),
        StructField("last_played", LongType(), True),
    ]
)

REVIEW_DATA_SCHEMA = StructType(
    [
        StructField("recommendationid", StringType(), True),
        StructField("author", REVIEW_AUTHOR_SCHEMA, True),
        StructField("language", StringType(), True),
        StructField("review", StringType(), True),
        StructField("timestamp_created", LongType(), True),
        StructField("timestamp_updated", LongType(), True),
        StructField("voted_up", BooleanType(), True),
        StructField("votes_up", IntegerType(), True),
        StructField("votes_funny", IntegerType(), True),
        # Steam returns both JSON strings and numbers for this score.
        StructField("weighted_vote_score", StringType(), True),
        StructField("comment_count", IntegerType(), True),
        StructField("steam_purchase", BooleanType(), True),
        StructField("received_for_free", BooleanType(), True),
        StructField("refunded", BooleanType(), True),
        StructField("written_during_early_access", BooleanType(), True),
    ]
)

STEAM_REVIEWS_BRONZE_SCHEMA = StructType(
    [
        StructField("appid", IntegerType(), True),
        StructField("game_name", StringType(), True),
        StructField("ingested_at", StringType(), True),
        StructField("page_number", IntegerType(), True),
        StructField("request_cursor", StringType(), True),
        StructField("review", REVIEW_DATA_SCHEMA, True),
    ]
)
