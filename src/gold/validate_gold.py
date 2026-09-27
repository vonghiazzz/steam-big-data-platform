def validate_join(
    reviews_df,
    games_df,
    gold_df
):

    reviews_count = reviews_df.count()

    gold_count = gold_df.count()


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
        gold_count / reviews_count
    ) * 100


    print("===== GOLD JOIN VALIDATION =====")

    print(
        "Silver Reviews:",
        reviews_count
    )

    print(
        "Gold Dataset:",
        gold_count
    )

    print(
        "Unmatched Reviews:",
        unmatched_count
    )

    print(
        f"Join Coverage: {coverage}%"
    )
