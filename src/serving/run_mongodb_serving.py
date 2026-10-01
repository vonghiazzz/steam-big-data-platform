"""Publish the historical Gold Analytics snapshot to MongoDB."""

import argparse
import json
import os
from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from serving.mongodb_loader import (
    COLLECTION_SPECS,
    build_documents,
    index_inventory,
    load_all_collections,
    sample_queries,
    validate_database,
)


ANALYTICS_ROOT = "/steam/gold/analytics"
ANALYTICS_PATHS = {
    name: f"{ANALYTICS_ROOT}/{name}" for name in COLLECTION_SPECS
}
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def create_spark() -> SparkSession:
    return SparkSession.builder.appName("SteamMongoDBServingV1").getOrCreate()


def read_analytics(spark: SparkSession) -> dict[str, DataFrame]:
    return {
        name: spark.read.parquet(path)
        for name, path in ANALYTICS_PATHS.items()
    }


def _require_columns(
    frame: DataFrame,
    dataset_name: str,
    columns: Iterable[str],
) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise RuntimeError(
            f"{dataset_name} missing columns: {', '.join(missing)}"
        )


def _validate_rate(frame: DataFrame, dataset_name: str) -> None:
    if "recommendation_rate" not in frame.columns:
        return
    invalid = frame.filter(
        F.col("recommendation_rate").isNull()
        | (F.col("recommendation_rate") < 0)
        | (F.col("recommendation_rate") > 1)
    ).count()
    if invalid:
        raise RuntimeError(
            f"{dataset_name} contains {invalid} invalid recommendation rates"
        )


def _review_total(
    frame: DataFrame,
    dataset_name: str,
    expected_total: int,
) -> int:
    _require_columns(frame, dataset_name, ("review_count",))
    total = frame.agg(F.sum("review_count").alias("total")).first()["total"]
    if total != expected_total:
        raise RuntimeError(
            f"{dataset_name} review total {total} != {expected_total}"
        )
    return int(total)


def validate_sources(
    datasets: dict[str, DataFrame],
) -> tuple[dict[str, int], dict[str, int]]:
    if set(datasets) != set(COLLECTION_SPECS):
        raise RuntimeError("Gold Analytics dataset set does not match Serving V1")

    counts: dict[str, int] = {}
    for name, spec in COLLECTION_SPECS.items():
        frame = datasets[name]
        if spec.key_field:
            _require_columns(frame, name, (spec.key_field,))
        count = frame.count()
        if count <= 0:
            raise RuntimeError(f"{name} source is empty")
        counts[name] = count
        _validate_rate(frame, name)

    profile = datasets["label_profile"].first()
    profile_fields = ("total_rows", "positive_count", "negative_count")
    _require_columns(datasets["label_profile"], "label_profile", profile_fields)
    expected_profile = {field: int(profile[field]) for field in profile_fields}
    expected_total = expected_profile["total_rows"]
    if expected_total <= 0:
        raise RuntimeError("label_profile total_rows must be positive")
    if (
        expected_profile["positive_count"]
        + expected_profile["negative_count"]
        != expected_total
    ):
        raise RuntimeError("label_profile labels do not reconcile to total_rows")

    for name in (
        "game_metrics",
        "playtime_metrics",
        "free_paid_metrics",
        "purchase_metrics",
    ):
        _review_total(datasets[name], name, expected_total)
    return counts, expected_profile


def collect_documents(
    datasets: dict[str, DataFrame],
) -> dict[str, list[dict]]:
    return {
        name: build_documents(name, frame.collect())
        for name, frame in datasets.items()
    }


def _json_default(value):
    if isinstance(value, Path):
        return str(value)
    return str(value)


def write_evidence(
    evidence_dir: Path,
    source_counts: dict[str, int],
    mongo_counts: dict[str, int],
    totals: dict[str, int],
    indexes: dict[str, dict],
    queries: dict,
    first_counts: dict[str, int],
    second_counts: dict[str, int],
) -> None:
    evidence_dir.mkdir(parents=True, exist_ok=True)
    count_lines = ["collection\tsource_count\tmongodb_count"]
    count_lines.extend(
        f"{name}\t{source_counts[name]}\t{mongo_counts[name]}"
        for name in COLLECTION_SPECS
    )
    (evidence_dir / "mongodb_collection_counts.txt").write_text(
        "\n".join(count_lines) + "\n",
        encoding="utf-8",
    )

    validation_lines = [
        "MONGODB SERVING V1",
        *(f"{name}={mongo_counts[name]}" for name in COLLECTION_SPECS),
        f"reviews={totals['reviews']}",
        f"positive={totals['positive']}",
        f"negative={totals['negative']}",
        f"game_reviews={totals['game_reviews']}",
        f"free_paid_reviews={totals['free_paid_reviews']}",
        f"purchase_reviews={totals['purchase_reviews']}",
        "MONGODB SERVING VALIDATION: PASS",
    ]
    (evidence_dir / "mongodb_validation.txt").write_text(
        "\n".join(validation_lines) + "\n",
        encoding="utf-8",
    )

    index_lines = ["collection\tindex_name\tkeys"]
    for collection_name in COLLECTION_SPECS:
        for index_name, details in sorted(indexes[collection_name].items()):
            index_lines.append(
                f"{collection_name}\t{index_name}\t{details.get('key')}"
            )
    (evidence_dir / "mongodb_indexes.txt").write_text(
        "\n".join(index_lines) + "\n",
        encoding="utf-8",
    )

    (evidence_dir / "mongodb_sample_queries.txt").write_text(
        json.dumps(queries, indent=2, sort_keys=True, default=_json_default)
        + "\n",
        encoding="utf-8",
    )

    idempotent = first_counts == second_counts == mongo_counts
    idempotency_lines = [
        "collection\trun_1\trun_2",
        *(
            f"{name}\t{first_counts[name]}\t{second_counts[name]}"
            for name in COLLECTION_SPECS
        ),
        f"MONGODB IDEMPOTENCY: {'PASS' if idempotent else 'FAIL'}",
    ]
    (evidence_dir / "mongodb_idempotency.txt").write_text(
        "\n".join(idempotency_lines) + "\n",
        encoding="utf-8",
    )
    if not idempotent:
        raise RuntimeError("MongoDB counts changed after the second load")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    )
    parser.add_argument(
        "--database",
        default=os.getenv("MONGO_DATABASE", "steam_analytics"),
    )
    parser.add_argument(
        "--evidence-dir",
        type=Path,
        default=PROJECT_ROOT / "evidence" / "serving",
    )
    return parser.parse_args()


def main() -> int:
    from pymongo import MongoClient

    args = parse_args()
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    client = None
    try:
        datasets = read_analytics(spark)
        source_counts, expected_profile = validate_sources(datasets)
        documents = collect_documents(datasets)

        client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5_000)
        client.admin.command("ping")
        database = client[args.database]

        load_all_collections(database, documents)
        first_counts, first_totals = validate_database(
            database,
            expected_counts=source_counts,
            expected_profile=expected_profile,
        )
        load_all_collections(database, documents)
        second_counts, totals = validate_database(
            database,
            expected_counts=source_counts,
            expected_profile=expected_profile,
        )
        if first_totals != totals:
            raise RuntimeError("MongoDB totals changed after the second load")

        queries = sample_queries(database)
        indexes = index_inventory(database)
        write_evidence(
            args.evidence_dir,
            source_counts,
            second_counts,
            totals,
            indexes,
            queries,
            first_counts,
            second_counts,
        )

        print("\n=== MONGODB SERVING V1 ===")
        print("\nSource:")
        for name in COLLECTION_SPECS:
            print(f"{name}={source_counts[name]}")
        print("\nMongoDB:")
        for name in COLLECTION_SPECS:
            print(f"{name}={second_counts[name]}")
        print(f"\nreviews={totals['reviews']}")
        print(f"positive={totals['positive']}")
        print(f"negative={totals['negative']}")
        print("\nMONGODB SERVING VALIDATION: PASS")
        print("MONGODB IDEMPOTENCY: PASS")
        return 0
    finally:
        if client is not None:
            client.close()
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
