import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


HISTORICAL_GOLD_PATH = os.getenv(
    "CURRENT_GOLD_HISTORICAL_PATH",
    "/steam/gold/base",
)

REALTIME_GOLD_PATH = os.getenv(
    "CURRENT_GOLD_REALTIME_PATH",
    "/steam/gold/base_incremental_v1",
)

CURRENT_GOLD_PATH = os.getenv(
    "CURRENT_GOLD_OUTPUT_PATH",
    "/steam/gold/base_current_v1",
)


def schema_map(df):
    return {
        field.name: field.dataType.simpleString()
        for field in df.schema.fields
    }


def require_unique_ids(df, name):
    rows = df.count()

    unique_ids = (
        df.select("recommendationid")
        .distinct()
        .count()
    )

    null_ids = df.filter(
        F.col("recommendationid").isNull()
    ).count()

    if null_ids:
        raise ValueError(
            f"{name} contains {null_ids} null recommendationid values"
        )

    if rows != unique_ids:
        raise ValueError(
            f"{name} recommendationid is not unique: "
            f"rows={rows}, unique_ids={unique_ids}"
        )

    return rows, unique_ids


def main():
    spark = (
        SparkSession.builder
        .appName("steam-current-gold-v1")
        .getOrCreate()
    )

    try:
        print("=== CURRENT GOLD V1 ===")
        print(f"historical={HISTORICAL_GOLD_PATH}")
        print(f"realtime={REALTIME_GOLD_PATH}")
        print(f"output={CURRENT_GOLD_PATH}")

        historical = spark.read.parquet(
            HISTORICAL_GOLD_PATH
        )

        realtime = spark.read.parquet(
            REALTIME_GOLD_PATH
        )

        if "recommendationid" not in historical.columns:
            raise ValueError(
                "Historical Gold missing recommendationid"
            )

        if "recommendationid" not in realtime.columns:
            raise ValueError(
                "Realtime Gold missing recommendationid"
            )

        historical_schema = schema_map(historical)
        realtime_schema = schema_map(realtime)

        historical_columns = set(historical.columns)
        realtime_columns = set(realtime.columns)

        missing_in_realtime = (
            historical_columns - realtime_columns
        )

        if missing_in_realtime:
            raise ValueError(
                "Realtime Gold missing historical columns: "
                + ", ".join(sorted(missing_in_realtime))
            )

        type_mismatches = []

        for column in historical.columns:
            if (
                historical_schema[column]
                != realtime_schema[column]
            ):
                type_mismatches.append(
                    (
                        column,
                        historical_schema[column],
                        realtime_schema[column],
                    )
                )

        if type_mismatches:
            raise ValueError(
                f"Schema type mismatch: {type_mismatches}"
            )

        hist_rows, hist_ids = require_unique_ids(
            historical,
            "historical",
        )

        rt_rows, rt_ids = require_unique_ids(
            realtime,
            "realtime",
        )

        overlap_count = (
            historical
            .select("recommendationid")
            .join(
                realtime.select("recommendationid"),
                "recommendationid",
                "inner",
            )
            .count()
        )

        # Output schema intentionally matches Historical Gold.
        # review_date and any other realtime-only partition/helper
        # columns are not part of the canonical Current Gold contract.
        realtime_aligned = realtime.select(
            *historical.columns
        )

        historical_ranked = historical.withColumn(
            "_source_rank",
            F.lit(0),
        )

        realtime_ranked = realtime_aligned.withColumn(
            "_source_rank",
            F.lit(1),
        )

        combined = historical_ranked.unionByName(
            realtime_ranked
        )

        # Historical Gold is the canonical batch source.
        # Realtime is additive.
        # If the same recommendationid exists in both,
        # keep the canonical historical row deterministically.
        window = Window.partitionBy(
            "recommendationid"
        ).orderBy(
            F.col("_source_rank").asc()
        )

        current = (
            combined
            .withColumn(
                "_row_number",
                F.row_number().over(window),
            )
            .filter(F.col("_row_number") == 1)
            .drop("_row_number", "_source_rank")
        )

        current_rows = current.count()

        current_unique_ids = (
            current
            .select("recommendationid")
            .distinct()
            .count()
        )

        expected_rows = (
            hist_ids
            + rt_ids
            - overlap_count
        )

        print()
        print("=== INPUT PROFILE ===")
        print(f"historical_rows={hist_rows}")
        print(f"historical_unique_ids={hist_ids}")
        print(f"realtime_rows={rt_rows}")
        print(f"realtime_unique_ids={rt_ids}")
        print(f"overlap_ids={overlap_count}")

        print()
        print("=== CURRENT PROFILE ===")
        print(f"expected_rows={expected_rows}")
        print(f"current_rows={current_rows}")
        print(
            "current_unique_ids="
            f"{current_unique_ids}"
        )

        if current_rows != expected_rows:
            raise ValueError(
                "Current Gold row-count invariant failed: "
                f"expected={expected_rows}, "
                f"actual={current_rows}"
            )

        if current_rows != current_unique_ids:
            raise ValueError(
                "Current Gold contains duplicate "
                "recommendationid values"
            )

        current.write.mode("overwrite").parquet(
            CURRENT_GOLD_PATH
        )

        print()
        print("=== WRITE COMPLETE ===")
        print(f"path={CURRENT_GOLD_PATH}")
        print(f"rows={current_rows}")
        print(
            f"unique_recommendationids="
            f"{current_unique_ids}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
