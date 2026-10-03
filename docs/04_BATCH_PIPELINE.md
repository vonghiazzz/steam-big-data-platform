# Batch Pipeline

## Scope and status

The historical batch path is implemented through verified HDFS Bronze. PySpark Bronze-to-Silver is **Implemented** (`src/processing/bronze_to_silver.py`, output at `/steam/silver/v1/` with reconciliation report at `/steam/silver/_reports/bronze_to_silver_v1.json`). Silver-to-Gold is **Planned**.

## Historical backfill

For the current experiment, `selected_50_games.jsonl` drives game-metadata preparation and review crawling. The crawler preserves source-near records by game and retains raw API pages for replay/debugging. Landing validation checks expected games, counts, identifiers, language, labels, and duplicates. Bronze-ready finalization produces a reproducible 50-game × 500-review handoff before HDFS upload.

In the scalable design, the same historical path applies only to each qualified `NEW` registry entry:

```text
NEW game
-> Python historical backfill
-> raw Steam API records
-> Ingestion Integrity Validation
-> HDFS Bronze
-> verification/reconciliation
-> mark ACTIVE
```

Ingestion Integrity Validation checks transport and structural integrity such as readable responses, parseable JSON, expected AppIDs, files, completeness, counts, and supported checksums. Historical records are not analytically cleaned before Bronze. Outlier handling, normalization, feature engineering, aggregation, and ML filtering belong after Bronze.

An existing `ACTIVE` game is handled by incremental polling and must not receive another full historical crawl during each weekly discovery cycle. A deliberate repair or rebuild is a separate auditable operation.

## Bronze to Silver — implemented

`src/processing/bronze_to_silver.py` runs as a PySpark job on the cluster client container (PySpark copied to `bda501-client:/tmp/pylibs`, `HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop`). Its responsibilities:

Run command (from repo root, after copying the script into the container):

```bash
docker cp src/processing/bronze_to_silver.py bda501-client:/tmp/jobs/
docker exec bda501-client sh -lc "PYTHONPATH=/tmp/pylibs HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop python3 /tmp/jobs/bronze_to_silver.py --bronze-root /steam/bronze --silver-root /steam/silver --master 'local[4]'"
```

1. Read Bronze game and review JSON/JSONL from HDFS.
2. Apply explicit PySpark schemas rather than schema inference alone.
3. Validate required keys and source relationships.
4. Convert booleans, numerics, arrays, and timestamps to stable types.
5. Deduplicate reviews by `recommendationid` using a documented rule.
6. Normalize game metadata structures needed for joins and analytics.
7. Write partitioned/organized Silver Parquet to HDFS.
8. Produce Bronze-versus-Silver reconciliation: inputs, accepted rows, rejected rows, duplicates, and output counts.

The pipeline fails clearly on broken contracts (exit code 1, zero accepted rows, or reconciliation mismatch) and keeps rejected records under `<silver>/<version>/rejected_*` with explicit reasons instead of silently dropping data.

First run evidence (v1, 2026-10-03): 50/50 games accepted, 25,000/25,000 reviews accepted, 0 rejected, 0 duplicates, read-back verified (50 distinct appids, 25,000 distinct recommendationids).

## Silver to Gold — planned

Silver reviews will join game metadata on `appid`. Planned Gold outputs include game-level summaries, genre analytics, behavior features, and a versioned ML-ready table. Derived fields may include `playtime_hours`, recommendation label, price buckets, and playtime buckets.

## Rebuild and reprocessing

Batch processing remains necessary even with a streaming path. Replay from immutable Bronze supports:

- new or corrected schemas
- updated cleaning/deduplication rules
- recovery from corrupted Silver/Gold output
- historical onboarding of a newly qualified game
- backfilling features introduced after the original crawl
- reproducible rebuilds for a named research snapshot

Reprocessing should write to a new version or controlled replacement path and record the rule/schema version used.
