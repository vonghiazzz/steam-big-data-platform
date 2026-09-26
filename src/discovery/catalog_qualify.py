import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from common.config import (
    CANDIDATES_PATH,
    CATALOG_PROBE_ROOT,
)

from common.jsonl import (
    read_jsonl,
    write_jsonl,
)


APPDETAILS_URL = (
    "https://store.steampowered.com/"
    "api/appdetails"
)


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 "
        "BDA501-Educational-Project"
    )
}


DEFAULT_REQUEST_DELAY_SECONDS = 0.8


MIN_RELEASE_AGE_DAYS = 30
MIN_TOTAL_REVIEWS = 1000



def utc_now() -> str:
    return (
        datetime.now(
            timezone.utc
        )
        .isoformat()
    )



def qualify_catalog(
    input_path: Path,
    output_dir: Path,
    delay: float,
):

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )


    raw_metadata_path = (
        output_dir
        / "metadata_probe_raw.jsonl"
    )


    eligible_path = (
        output_dir
        / "eligible_games.jsonl"
    )


    candidates = read_jsonl(
        input_path
    )


    print("=" * 80)
    print(
        "STEAM CATALOG METADATA QUALIFICATION"
    )
    print("=" * 80)

    print(
        "Candidates:",
        len(candidates)
    )


    eligible = []


    success_count = 0
    metadata_failed = 0
    non_game_count = 0
    coming_soon_count = 0
    release_failed_count = 0



    with raw_metadata_path.open(
        "w",
        encoding="utf-8",
    ) as raw_file:


        for index, candidate in enumerate(
            candidates,
            start=1,
        ):


            appid = candidate["appid"]


            print(
                f"[{index:03d}/"
                f"{len(candidates):03d}] "
                f"appid={appid}",
                end=" ",
                flush=True,
            )


            try:

                response = requests.get(
                    APPDETAILS_URL,
                    params={
                        "appids": appid,
                        "l": "english",
                        "cc": "vn",
                    },
                    headers=HEADERS,
                    timeout=30,
                )


                print(
                    f"HTTP={response.status_code}",
                    end=" ",
                )


                response.raise_for_status()


                payload = response.json()

                # Steam may return a response key that differs from the
                # requested appid. The authoritative identity is
                # data.steam_appid inside the response item.
                app = payload.get(
                    str(appid),
                    {},
                )

                if not isinstance(app, dict) or not app.get("success"):
                    app = {}

                    for candidate_app in payload.values():
                        if not isinstance(candidate_app, dict):
                            continue

                        candidate_data = candidate_app.get("data")
                        if not isinstance(candidate_data, dict):
                            continue

                        if (
                            candidate_app.get("success") is True
                            and candidate_data.get("steam_appid") == appid
                        ):
                            app = candidate_app
                            break


                raw_record = {
                    "appid": appid,
                    "collected_at": utc_now(),
                    "source": "steam_appdetails",
                    "success": app.get(
                        "success",
                        False,
                    ),
                    "data": app.get(
                        "data"
                    ),
                }


                raw_file.write(
                    json.dumps(
                        raw_record,
                        ensure_ascii=False,
                    )
                    + "\n"
                )


                raw_file.flush()



                if not app.get(
                    "success"
                ):

                    metadata_failed += 1

                    print(
                        "→ success=false"
                    )

                    time.sleep(delay)

                    continue



                success_count += 1



                data = app.get(
                    "data",
                    {},
                )



                product_type = data.get(
                    "type"
                )


                if product_type != "game":

                    non_game_count += 1

                    print(
                        f"→ excluded type={product_type}"
                    )

                    time.sleep(delay)

                    continue



                release_info = data.get(
                    "release_date",
                    {}
                )


                coming_soon = release_info.get(
                    "coming_soon"
                )


                if coming_soon is True:

                    coming_soon_count += 1

                    print(
                        "→ excluded coming_soon"
                    )

                    time.sleep(delay)

                    continue



                release_date_text = release_info.get(
                    "date"
                )


                if not release_date_text:

                    release_failed_count += 1

                    print(
                        "→ excluded missing release date"
                    )

                    time.sleep(delay)

                    continue



                try:

                    release_date = datetime.strptime(
                        release_date_text,
                        "%d %b, %Y",
                    ).replace(
                        tzinfo=timezone.utc
                    )


                    age_days = (
                        datetime.now(
                            timezone.utc
                        )
                        - release_date
                    ).days



                    if age_days < MIN_RELEASE_AGE_DAYS:

                        print(
                            "→ excluded "
                            f"release_age={age_days} days"
                        )

                        time.sleep(delay)

                        continue



                except ValueError:

                    release_failed_count += 1

                    print(
                        "→ excluded invalid release date"
                    )

                    time.sleep(delay)

                    continue



                price_overview = data.get(
                    "price_overview"
                )



                game = {

                    "catalog_rank": index,

                    "appid": appid,

                    "name": data.get(
                        "name"
                    ),

                    "type": product_type,

                    "required_age": data.get(
                        "required_age"
                    ),

                    "is_free": data.get(
                        "is_free"
                    ),


                    "developers": data.get(
                        "developers",
                        [],
                    ),


                    "publishers": data.get(
                        "publishers",
                        [],
                    ),


                    "genres": [
                        item.get(
                            "description"
                        )
                        for item in data.get(
                            "genres",
                            []
                        )
                        if item.get(
                            "description"
                        )
                    ],


                    "categories": [
                        item.get(
                            "description"
                        )
                        for item in data.get(
                            "categories",
                            []
                        )
                        if item.get(
                            "description"
                        )
                    ],


                    "release_date": release_date_text,

                    "release_age_days": age_days,

                    "coming_soon": False,


                    "price_overview": price_overview,


                    "search_title": candidate.get(
                        "title"
                    ),


                    "search_release_text": candidate.get(
                        "search_release_text"
                    ),


                    "search_review_summary": candidate.get(
                        "search_review_summary"
                    ),

                }



                eligible.append(
                    game
                )


                print(
                    "→ KEEP:",
                    game["name"]
                )



            except Exception as exc:

                print()

                print(
                    "STOPPED:",
                    exc
                )

                print(
                    "Raw metadata already written:",
                    raw_metadata_path
                )

                raise



            time.sleep(delay)



    write_jsonl(
        eligible_path,
        eligible,
    )



    print()
    print("=" * 80)
    print("FINAL VALIDATION")
    print("=" * 80)


    print(
        "Candidates          :",
        len(candidates),
    )


    print(
        "Metadata success    :",
        success_count,
    )


    print(
        "Metadata failed     :",
        metadata_failed,
    )


    print(
        "Excluded non-game   :",
        non_game_count,
    )


    print(
        "Excluded coming soon:",
        coming_soon_count,
    )


    print(
        "Invalid release     :",
        release_failed_count,
    )


    print(
        "Eligible games      :",
        len(eligible),
    )



    print()

    print(
        "=== FIRST 15 ELIGIBLE ==="
    )


    for game in eligible[:15]:

        print(
            game["catalog_rank"],
            "|",
            game["appid"],
            "|",
            game["name"],
            "|",
            game["release_date"],
            "|",
            game["release_age_days"],
            "days"
        )



    print()

    print(
        "Outputs:"
    )

    print(
        raw_metadata_path
    )

    print(
        eligible_path
    )




def main():

    parser = argparse.ArgumentParser()


    parser.add_argument(
        "--input-path",
        type=Path,
        default=CANDIDATES_PATH,
    )


    parser.add_argument(
        "--output-dir",
        type=Path,
        default=CATALOG_PROBE_ROOT,
    )


    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_REQUEST_DELAY_SECONDS,
    )


    args = parser.parse_args()



    if args.delay < 0:

        parser.error(
            "--delay must be >= 0"
        )



    qualify_catalog(
        input_path=args.input_path,
        output_dir=args.output_dir,
        delay=args.delay,
    )




if __name__ == "__main__":

    main()