# Steam Bronze PySpark Ingestion Guide

## Purpose

This slice validates that PySpark can read the existing raw Steam Bronze JSONL
records with explicit nested schemas. It prints schemas, row counts, and a small
set of integrity checks. It does not transform data or write Parquet, Silver, or
Gold output.

## Canonical Bronze inputs

The only supported HDFS Bronze locations are:

- Games: `/steam/bronze/games`
- Reviews: `/steam/bronze/reviews`

HDFS Bronze is provisioned by the existing authoritative uploader:
`src/hdfs/upload_bronze.sh`. This reader is not an alternative upload flow.

The expected full canonical baseline is:

- 50 game records
- 25,000 review records
- 25,000 populated `review.recommendationid` values
- `review.voted_up = true`: 18,321
- `review.voted_up = false`: 6,679

## Explicit raw schemas

`src/schemas/steam_bronze_schema.py` preserves the JSONL wrappers:

- Game attributes remain nested below `data`.
- Review attributes remain nested below `review`.
- `data.categories` and `data.genres` are arrays of structs.
- Raw date strings and epoch values are not converted into Silver types.

## Run the validation

Use the already-running Spark/Hadoop environment. From the repository root,
run the reader in the Spark container with `src` on Python's import path:

```bash
docker exec \
  -e PYTHONPATH=/workspace/src \
  steam-spark \
  /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  /workspace/src/processing/bronze_ingestion.py
```

The source of truth for this validation is:
`src/processing/bronze_ingestion.py`. No duplicate Bronze reader is required.

Successful output includes:

```text
Games count: 50
Reviews count: 25000
[PASS] every review has populated review.recommendationid
[PASS] review.voted_up distribution is {True: 18321, False: 6679}
```

If canonical HDFS Bronze has not yet been provisioned, use the existing uploader
only when its expected local handoff is present:

```bash
CONTAINER=steam-namenode bash src/hdfs/upload_bronze.sh
```

Do not create a second HDFS namespace or a second upload procedure.

## Next slices

Silver cleaning and Gold aggregation are separate later slices. They are
intentionally outside this Bronze-read validation.
