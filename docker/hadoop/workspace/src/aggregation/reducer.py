def reducer(appid, reviews):

    total_reviews = 0
    positive = 0
    negative = 0

    total_playtime = 0


    game_name = None


    for review in reviews:

        total_reviews += 1

        game_name = review["game_name"]


        if review["voted_up"]:
            positive += 1
        else:
            negative += 1


        total_playtime += (
            review["playtime_hours"] or 0
        )


    avg_playtime = (
        total_playtime / total_reviews
        if total_reviews > 0
        else 0
    )


    return {

        "appid": appid,

        "game_name": game_name,

        "total_reviews": total_reviews,

        "positive_reviews": positive,

        "negative_reviews": negative,

        "avg_playtime_hours": avg_playtime
    }