#!/usr/bin/env python3

import sys
import json

for line in sys.stdin:
    try:
        row = json.loads(line)

        appid = row["appid"]

        value = {
            "game_name": row["game_name"],
            "voted_up": row["voted_up"],
            "playtime_hours": row["playtime_hours"]
        }

        print(f"{appid}\t{json.dumps(value)}")

    except Exception:
        continue
