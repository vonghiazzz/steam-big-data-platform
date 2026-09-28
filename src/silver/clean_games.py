"""Flatten canonical Steam Bronze game records for the Silver layer."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_games(games_df: DataFrame) -> DataFrame:
    """Return one Silver row for every canonical Bronze game row.

    Steam exposes both ``price_overview.final`` and package-option prices in
    the currency's minor unit. Dividing by 100 converts either source to the
    major unit. Free games are always represented as 0.0 and never inspect
    package purchases. Paid games prefer ``price_overview`` and use only the
    unique, non-recurring default package as a fallback.

    This transformation deliberately does not filter or deduplicate rows.
    The pipeline rejects invalid or duplicate keys before writing.
    """
    is_free = F.col("data.is_free")
    final_price_minor = F.col("data.price_overview.final")
    valid_default_groups = F.filter(
        F.col("data.package_groups"),
        lambda group: (
            (group["name"] == F.lit("default"))
            & (group["title"] == F.concat(F.lit("Buy "), F.col("data.name")))
            & (group["is_recurring_subscription"] == F.lit("false"))
            & (F.size(group["subs"]) > 0)
        ),
    )
    has_unique_default_group = F.size(valid_default_groups) == 1
    default_group = F.element_at(valid_default_groups, 1)
    default_option = F.element_at(default_group["subs"], 1)
    default_price_minor = default_option["price_in_cents_with_discount"]
    default_option_text = default_option["option_text"]

    price = (
        F.when(is_free == F.lit(True), F.lit(0.0))
        .when(
            (is_free == F.lit(False)) & final_price_minor.isNotNull(),
            final_price_minor.cast("double") / F.lit(100.0),
        )
        .when(
            (is_free == F.lit(False))
            & final_price_minor.isNull()
            & has_unique_default_group
            & default_price_minor.isNotNull(),
            default_price_minor.cast("double") / F.lit(100.0),
        )
        .otherwise(F.lit(None).cast("double"))
    )

    currency = (
        F.when(
            (is_free == F.lit(False)) & final_price_minor.isNotNull(),
            F.col("data.price_overview.currency"),
        )
        .when(
            (is_free == F.lit(False))
            & final_price_minor.isNull()
            & has_unique_default_group
            & default_price_minor.isNotNull()
            & (F.instr(default_option_text, "₫") > 0),
            F.lit("VND"),
        )
        .otherwise(F.lit(None).cast("string"))
    )

    return games_df.select(
        F.col("appid").alias("appid"),
        F.col("data.name").alias("game_name"),
        is_free.alias("is_free"),
        price.alias("price"),
        currency.alias("currency"),
        F.transform(
            F.col("data.genres"),
            lambda genre: genre["description"],
        ).alias("genres"),
        F.transform(
            F.col("data.categories"),
            lambda category: category["description"],
        ).alias("categories"),
        F.col("data.platforms").alias("platforms"),
        F.col("data.release_date.date").alias("release_date"),
    )
