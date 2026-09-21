import json

from steam.common.config import (
    LANDING_STATE_PATH,
    SELECTED_GAMES_PATH,
    TARGET_GAME_COUNT,
)
from steam.common.jsonl import iter_jsonl


def main():
    selected = list(
        iter_jsonl(
            SELECTED_GAMES_PATH
        )
    )

    selected_by_appid = {
        str(row["appid"]): row
        for row in selected
    }

    state = json.loads(
        LANDING_STATE_PATH.read_text(
            encoding="utf-8"
        )
    )

    print("=" * 100)
    print(
        "STEAM INCOMPLETE GAME AUDIT"
    )
    print("=" * 100)

    print(
        f"{'APPID':<12}"
        f"{'STATUS':<23}"
        f"{'CRAWLED':>8}"
        f"{'SUMMARY':>12}"
        f"{'RATE':>10}  "
        f"NAME"
    )

    print("-" * 100)

    incomplete = []

    for appid, game_state in (
        state.get(
            "games",
            {},
        ).items()
    ):
        status = game_state.get(
            "status"
        )

        if status == "TARGET_REACHED":
            continue

        selected_row = (
            selected_by_appid.get(
                appid,
                {},
            )
        )

        crawled = game_state.get(
            "unique_reviews",
            0,
        )

        summary_total = (
            selected_row.get(
                "total_reviews",
                0,
            )
        )

        rate = selected_row.get(
            "overall_positive_rate"
        )

        incomplete.append(
            (
                int(appid),
                status,
                crawled,
                summary_total,
                rate,
                selected_row.get(
                    "name",
                    game_state.get(
                        "name",
                        "UNKNOWN",
                    ),
                ),
            )
        )

    incomplete.sort(
        key=lambda row: row[2]
    )

    for (
        appid,
        status,
        crawled,
        summary_total,
        rate,
        name,
    ) in incomplete:
        rate_text = (
            f"{rate:.3f}"
            if isinstance(
                rate,
                (int, float),
            )
            else "N/A"
        )

        print(
            f"{appid:<12}"
            f"{str(status):<23}"
            f"{crawled:>8}"
            f"{summary_total:>12}"
            f"{rate_text:>10}  "
            f"{name}"
        )

    print()

    print(
        "Incomplete games:",
        len(incomplete),
    )

    print(
        "Target reached :",
        TARGET_GAME_COUNT
        - len(incomplete),
    )


if __name__ == "__main__":
    main()
