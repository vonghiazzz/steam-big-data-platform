from pyspark.sql import functions as F


def create_gold_dataset(
    reviews_df,
    games_df
):

    games_selected = games_df.select(
        "appid",
        "genres",
        "primary_genre",
        "is_free",
        "price",
        "release_year",
        "overall_positive_rate"
    )

    gold_df = reviews_df.join(
        games_selected,
        on="appid",
        how="inner"
    )

    return gold_df



def validate_join(
    reviews_df,
    games_df,
    gold_df
):

    reviews_count = reviews_df.count()

    gold_count = gold_df.count()


    # unmatched reviews
    unmatched_reviews = (
        reviews_df
        .join(
            games_df.select("appid"),
            on="appid",
            how="left_anti"
        )
    )


    unmatched_count = unmatched_reviews.count()


    coverage = (
        gold_count / reviews_count * 100
    )


    print("========== GOLD JOIN VALIDATION ==========")

    print(
        f"Silver Reviews: {reviews_count}"
    )

    print(
        f"Silver Games: {games_df.count()}"
    )

    print(
        f"Gold Dataset: {gold_count}"
    )

    print(
        f"Unmatched Reviews: {unmatched_count}"
    )

    print(
        f"Join Coverage: {coverage:.2f}%"
    )


    return unmatched_reviews