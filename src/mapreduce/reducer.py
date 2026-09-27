#!/usr/bin/env python3

import sys
import json

current_appid = None
total = 0
positive = 0
total_playtime = 0
game_name = None


def emit():
    if current_appid is not None:
        print(json.dumps({
            "appid": current_appid,
            "game_name": game_name,
            "total_reviews": total,
            "positive_reviews": positive,
            "avg_playtime_hours": round(total_playtime / total, 2)
        }))


for line in sys.stdin:
    try:
        appid, value = line.strip().split("\t", 1)
        data = json.loads(value)

        if current_appid != appid:
            emit()

            current_appid = appid
            total = 0
            positive = 0
            total_playtime = 0
            game_name = data["game_name"]

        total += 1

        if data["voted_up"]:
            positive += 1

        total_playtime += data["playtime_hours"]

    except Exception:
        continue


emit()