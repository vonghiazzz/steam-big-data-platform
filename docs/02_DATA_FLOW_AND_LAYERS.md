# Data Flow and Lake Layers

## Local staging flow

The implemented local flow is:

```text
Steam API
-> data/raw/steam/landing
-> landing validation
-> data/raw/steam/bronze_ready
```

`landing/` contains crawler output and restart/debug evidence. `bronze_ready/` is a validated, selected-scope handoff prepared for HDFS upload. These local folders are staging, crawl, and backup locations. They are not the authoritative Bronze/Silver/Gold lake.

## Authoritative HDFS flow

```text
local bronze_ready
-> HDFS Bronze
-> PySpark Bronze-to-Silver
-> HDFS Silver (Parquet)
-> PySpark Silver-to-Gold
-> HDFS Gold
```

The configured lake roots are:

- Bronze: `/user/hadoop/steam/bronze`
- Silver: `/user/hadoop/steam/silver`
- Gold: `/user/hadoop/steam/gold`

Only Bronze upload/verification is currently **Implemented**. Bronze-to-Silver is the immediate **In Progress** checkpoint; later transformations are **Planned**.

## Data contracts

### Bronze — raw selected-scope data

Bronze preserves immutable, replayable records for the managed scope:

- raw Steam game metadata
- historical review records
- retained raw review API pages
- planned raw events under a `stream_events` area
- source identifiers and ingestion context needed for replay/audit

Selecting or managing scope before ingestion does not make Bronze “clean.” Bronze applies no analytical transformation and must not be described as Silver-like curated data.

### Silver — cleaned and typed records

Silver applies an explicit schema and produces validated Parquet datasets. Its contract includes:

- typed identifiers, booleans, numerics, and timestamps
- required-key validation for `appid` and `recommendationid`
- duplicate handling, primarily by `recommendationid` for reviews
- normalized game metadata fields such as genres/platforms/categories
- auditable rejected/invalid counts
- row-count and key reconciliation against Bronze

Batch and streaming data must converge on compatible Silver contracts.

### Gold — analytics and ML-ready data

Gold contains purpose-built outputs rather than raw records:

- game- and genre-level aggregates
- analytical tables for Spark SQL and visualization
- joined review/game feature tables
- versioned ML-ready datasets and prediction outputs
- batch results plus planned incremental updates

Gold datasets should retain lineage to the Silver snapshot or streaming batch that produced them.

## Validation boundaries

Each transition records input/output counts, duplicate counts, rejected records, schema results, and relevant keys. Bronze remains replayable; a changed rule should rebuild Silver/Gold without rewriting raw source history.
