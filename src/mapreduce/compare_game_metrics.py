#!/usr/bin/env python3
"""Compare Hadoop Streaming game metrics with Spark Gold Analytics."""

import argparse
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


EXPECTED_APPIDS = 50
EXPECTED_REVIEWS = 25_000
EXPECTED_POSITIVES = 18_321
EXPECTED_NEGATIVES = 6_679


@dataclass(frozen=True)
class GameMetric:
    appid: int
    review_count: int
    positive_reviews: int
    negative_reviews: int
    recommendation_rate: float


@dataclass(frozen=True)
class Comparison:
    missing_in_mapreduce: tuple[int, ...]
    unexpected_in_mapreduce: tuple[int, ...]
    count_mismatches: tuple[int, ...]
    positive_mismatches: tuple[int, ...]
    negative_mismatches: tuple[int, ...]
    rate_mismatches: tuple[int, ...]


def _validate_metric(metric: GameMetric, source: str) -> None:
    if metric.appid <= 0:
        raise ValueError(f"{source}: invalid appid {metric.appid}")
    if metric.review_count <= 0:
        raise ValueError(f"{source}: invalid review_count for {metric.appid}")
    if metric.positive_reviews < 0 or metric.negative_reviews < 0:
        raise ValueError(f"{source}: negative label count for {metric.appid}")
    if metric.positive_reviews + metric.negative_reviews != metric.review_count:
        raise ValueError(f"{source}: labels do not reconcile for {metric.appid}")
    if not math.isfinite(metric.recommendation_rate):
        raise ValueError(f"{source}: non-finite rate for {metric.appid}")
    if not 0 <= metric.recommendation_rate <= 1:
        raise ValueError(f"{source}: rate outside [0, 1] for {metric.appid}")


def read_metrics_tsv(path: str | Path) -> dict[int, GameMetric]:
    metrics: dict[int, GameMetric] = {}
    source = str(path)
    with Path(path).open(encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            if not raw_line.strip():
                continue
            fields = raw_line.rstrip("\r\n").split("\t")
            if len(fields) != 5:
                raise ValueError(
                    f"{source}:{line_number}: expected five TSV fields"
                )
            try:
                metric = GameMetric(
                    appid=int(fields[0]),
                    review_count=int(fields[1]),
                    positive_reviews=int(fields[2]),
                    negative_reviews=int(fields[3]),
                    recommendation_rate=float(fields[4]),
                )
            except ValueError as exc:
                raise ValueError(
                    f"{source}:{line_number}: invalid numeric field"
                ) from exc
            _validate_metric(metric, f"{source}:{line_number}")
            if metric.appid in metrics:
                raise ValueError(f"{source}: duplicate appid {metric.appid}")
            metrics[metric.appid] = metric
    return metrics


def write_metrics_tsv(
    path: str | Path,
    metrics: Iterable[GameMetric],
) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(metrics, key=lambda metric: metric.appid)
    with target.open("w", encoding="utf-8", newline="\n") as handle:
        for metric in rows:
            _validate_metric(metric, str(target))
            handle.write(
                f"{metric.appid}\t{metric.review_count}\t"
                f"{metric.positive_reviews}\t{metric.negative_reviews}\t"
                f"{metric.recommendation_rate:.12f}\n"
            )


def load_spark_reference(hdfs_path: str) -> dict[int, GameMetric]:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(
        "SteamMapReduceSparkCrossCheck"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        frame = spark.read.parquet(hdfs_path)
        required = {
            "appid",
            "review_count",
            "positive_reviews",
            "negative_reviews",
            "recommendation_rate",
        }
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(
                "Spark game_metrics missing columns: " + ", ".join(missing)
            )
        metrics: dict[int, GameMetric] = {}
        for row in frame.select(*sorted(required)).collect():
            metric = GameMetric(
                appid=int(row["appid"]),
                review_count=int(row["review_count"]),
                positive_reviews=int(row["positive_reviews"]),
                negative_reviews=int(row["negative_reviews"]),
                recommendation_rate=float(row["recommendation_rate"]),
            )
            _validate_metric(metric, "Spark game_metrics")
            if metric.appid in metrics:
                raise ValueError(f"Spark game_metrics duplicate appid {metric.appid}")
            metrics[metric.appid] = metric
        return metrics
    finally:
        spark.stop()


def compare_metrics(
    mapreduce: dict[int, GameMetric],
    spark: dict[int, GameMetric],
    tolerance: float = 1e-12,
) -> Comparison:
    mapreduce_appids = set(mapreduce)
    spark_appids = set(spark)
    common = sorted(mapreduce_appids & spark_appids)
    return Comparison(
        missing_in_mapreduce=tuple(sorted(spark_appids - mapreduce_appids)),
        unexpected_in_mapreduce=tuple(sorted(mapreduce_appids - spark_appids)),
        count_mismatches=tuple(
            appid
            for appid in common
            if mapreduce[appid].review_count != spark[appid].review_count
        ),
        positive_mismatches=tuple(
            appid
            for appid in common
            if mapreduce[appid].positive_reviews != spark[appid].positive_reviews
        ),
        negative_mismatches=tuple(
            appid
            for appid in common
            if mapreduce[appid].negative_reviews != spark[appid].negative_reviews
        ),
        rate_mismatches=tuple(
            appid
            for appid in common
            if not math.isclose(
                mapreduce[appid].recommendation_rate,
                spark[appid].recommendation_rate,
                rel_tol=0.0,
                abs_tol=tolerance,
            )
        ),
    )


def _detail_lines(
    comparison: Comparison,
    mapreduce: dict[int, GameMetric],
    spark: dict[int, GameMetric],
) -> list[str]:
    details: list[str] = []
    for appid in comparison.missing_in_mapreduce:
        details.append(f"appid={appid} missing_in=mapreduce spark={spark[appid]}")
    for appid in comparison.unexpected_in_mapreduce:
        details.append(
            f"appid={appid} missing_in=spark mapreduce={mapreduce[appid]}"
        )
    fields = (
        ("review_count", comparison.count_mismatches),
        ("positive_reviews", comparison.positive_mismatches),
        ("negative_reviews", comparison.negative_mismatches),
        ("recommendation_rate", comparison.rate_mismatches),
    )
    for field, appids in fields:
        for appid in appids:
            details.append(
                f"appid={appid} field={field} "
                f"spark={getattr(spark[appid], field)} "
                f"mapreduce={getattr(mapreduce[appid], field)}"
            )
    return details


def build_report(
    mapreduce: dict[int, GameMetric],
    spark: dict[int, GameMetric],
    comparison: Comparison,
) -> tuple[str, bool]:
    reviews = sum(metric.review_count for metric in mapreduce.values())
    positives = sum(metric.positive_reviews for metric in mapreduce.values())
    negatives = sum(metric.negative_reviews for metric in mapreduce.values())
    passed = (
        len(mapreduce) == EXPECTED_APPIDS
        and len(spark) == EXPECTED_APPIDS
        and reviews == EXPECTED_REVIEWS
        and positives == EXPECTED_POSITIVES
        and negatives == EXPECTED_NEGATIVES
        and not comparison.missing_in_mapreduce
        and not comparison.unexpected_in_mapreduce
        and not comparison.count_mismatches
        and not comparison.positive_mismatches
        and not comparison.negative_mismatches
        and not comparison.rate_mismatches
    )
    lines = [
        "MAPREDUCE VS SPARK",
        f"appids={len(mapreduce)}",
        f"reviews={reviews}",
        f"positive={positives}",
        f"negative={negatives}",
        f"missing_appids={len(comparison.missing_in_mapreduce)}",
        f"unexpected_appids={len(comparison.unexpected_in_mapreduce)}",
        f"count_mismatches={len(comparison.count_mismatches)}",
        f"positive_mismatches={len(comparison.positive_mismatches)}",
        f"negative_mismatches={len(comparison.negative_mismatches)}",
        f"rate_mismatches={len(comparison.rate_mismatches)}",
    ]
    details = _detail_lines(comparison, mapreduce, spark)
    if details:
        lines.extend(["", "DIFF DETAILS", *details])
    lines.extend(["", f"CROSS-CHECK: {'PASS' if passed else 'FAIL'}"])
    return "\n".join(lines) + "\n", passed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mapreduce-output", required=True)
    reference = parser.add_mutually_exclusive_group(required=True)
    reference.add_argument("--spark-reference-hdfs")
    reference.add_argument("--spark-reference-tsv")
    parser.add_argument("--spark-reference-output")
    parser.add_argument("--comparison-output")
    parser.add_argument("--tolerance", type=float, default=1e-12)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    mapreduce = read_metrics_tsv(args.mapreduce_output)
    if args.spark_reference_hdfs:
        spark = load_spark_reference(args.spark_reference_hdfs)
    else:
        spark = read_metrics_tsv(args.spark_reference_tsv)
    if args.spark_reference_output:
        write_metrics_tsv(args.spark_reference_output, spark.values())

    comparison = compare_metrics(mapreduce, spark, args.tolerance)
    report, passed = build_report(mapreduce, spark, comparison)
    print(report, end="")
    if args.comparison_output:
        target = Path(args.comparison_output)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(report, encoding="utf-8")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
