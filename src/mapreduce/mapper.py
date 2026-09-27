#!/usr/bin/env python3

import sys
import json


for line in sys.stdin:
    try:
        row = json.loads(line)

        appid = row["appid"]
        voted = row["recommendation_label"]

        playtime = row.get("playtime_hours", 0)

        print(f"{appid}\t{voted}\t{playtime}")

    except Exception as e:
        continue