import json
from datetime import datetime


def create_manifest(
    dataset_version,
    gold_paths,
    output_file
):

    manifest = {

        "dataset_name": "Steam Game Analytics Dataset",

        "version": dataset_version,

        "created_at":
            datetime.utcnow().isoformat(),

        "layers": {

            "gold": {

                "base":
                    gold_paths["base"],

                "game_aggregation":
                    gold_paths["game_aggregation"]

            }

        },

        "purpose": [

            "analytics",

            "machine_learning"

        ],

        "format": "parquet"

    }


    with open(output_file,"w") as f:

        json.dump(
            manifest,
            f,
            indent=4
        )


    print(
        "Manifest created:",
        output_file
    )