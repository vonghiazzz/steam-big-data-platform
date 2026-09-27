#!/usr/bin/env python3

import sys
import json


current_appid = None
reviews = []


def emit(appid, reviews):

    if not reviews:
        return

    total_reviews = len(reviews)

    positive = sum(
        1 for r in reviews 
        if r["voted_up"]
    )

    negative = total_reviews - positive

    total_playtime = sum(
        r["playtime_hours"] or 0 
        for r in reviews
    )

    result = {
        "appid": appid,
        "game_name": reviews[0]["game_name"],
        "total_reviews": total_reviews,
        "positive_reviews": positive,
        "negative_reviews": negative,
        "avg_playtime_hours": round(
            total_playtime / total_reviews,
            2
        )
    }

    print(json.dumps(result))


for line in sys.stdin:

    try:
        appid, value = line.strip().split("\t", 1)

        review = json.loads(value)

        if current_appid is None:
            current_appid = appid


        if appid != current_appid:

            emit(
                current_appid,
                reviews
            )

            current_appid = appid
            reviews = []


        reviews.append(review)


    except Exception:
        continue


# emit last group
emit(
    current_appid,
    reviews
)
