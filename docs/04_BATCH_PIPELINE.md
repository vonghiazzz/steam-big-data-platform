# Batch Pipeline

## Scope and status

The historical batch path is implemented through verified HDFS Bronze. PySpark Bronze-to-Silver is the immediate **In Progress** checkpoint. Silver-to-Gold is **Planned**.

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

## Bronze to Silver — next checkpoint

`src/batch/bronze_to_silver.py` is not yet implemented. Its required responsibilities are:

1. Read Bronze game and review JSON/JSONL from HDFS.
2. Apply explicit PySpark schemas rather than schema inference alone.
3. Validate required keys and source relationships.
4. Convert booleans, numerics, arrays, and timestamps to stable types.
5. Deduplicate reviews by `recommendationid` using a documented rule.
6. Normalize game metadata structures needed for joins and analytics.
7. Write partitioned/organized Silver Parquet to HDFS.
8. Produce Bronze-versus-Silver reconciliation: inputs, accepted rows, rejected rows, duplicates, and output counts.

The pipeline should fail clearly on broken contracts and keep rejected-record evidence instead of silently dropping data.

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
