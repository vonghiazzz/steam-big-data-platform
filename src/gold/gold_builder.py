from pyspark.sql import functions as F


def build_gold_dataset(reviews_df, games_df):

    gold_df = (
        reviews_df.alias("r")
        .join(
            games_df.drop("game_name"),
            on="appid",
            how="inner"
        )
        .select(

            # key
            F.col("r.appid"),

            # game info
            F.col("g.game_name"),
            F.col("g.genres"),
            F.col("g.primary_genre"),
            F.col("g.is_free"),
            F.col("g.price"),
            F.col("g.release_year"),
            F.col("g.total_reviews"),
            F.col("g.overall_positive_rate"),

            # review info
            F.col("r.recommendationid"),
            F.col("r.recommendation_label"),
            F.col("r.steam_purchase"),
            F.col("r.received_for_free"),
            F.col("r.playtime_hours"),
            F.col("r.language_clean"),
            F.col("r.review_year")
        )
    )

    return gold_df