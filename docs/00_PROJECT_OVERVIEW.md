# Project Overview

## Goal and research question

This BDA501 final project builds a reproducible Big Data workflow for Steam game and player-behavior analytics. The main research question is:

> Can player behavior and game characteristics be used to predict whether a Steam user recommends a game?

The primary prediction target is `voted_up`. The primary model intentionally excludes review text and focuses on behavioral and game-metadata features.

## Current research snapshot

The implemented experiment uses a versioned, fixed cohort:

- 50 selected games in `data/raw/steam/selected_50_games.jsonl`
- 25,000 reviews, with 500 reviews per selected game
- 18,321 positive and 6,679 negative `voted_up` labels
- raw game metadata, review records, and retained API-page evidence

This snapshot supports reproducible EDA, ML, and MapReduce-versus-Spark validation. It is not a permanent system limit. The scalable design maintains a Game Registry / Watchlist so future discovery cycles can onboard additional games.

## Status

| Area | Status | Current evidence or intent |
|---|---|---|
| Catalog discovery and qualification | **Implemented** | Steam candidates, probes, versioned policy configuration, explainable evaluator |
| Discovery Control Plane V1 | **Implemented** | Local JSONL registry, reconciliation, onboarding plan, run report |
| Initial game selection | **Implemented** | Fixed 50-game research cohort |
| Historical ingestion | **Implemented** | Review crawler and game-metadata preparation |
| Landing and bronze-ready validation | **Implemented** | Validation and finalization scripts with reconciliation |
| HDFS Bronze upload and verification | **Implemented** | Raw selected-scope data verified under HDFS Bronze |
| PySpark Bronze-to-Silver | **In Progress / next checkpoint** | Implementation file does not yet exist |
| Silver-to-Gold, MapReduce, Spark SQL / EDA | **Planned** | Designs documented in this folder |
| Production registry automation | **Design-only** | Local V1 exists; database service and scheduler are not implemented |
| Kafka and Structured Streaming | **Design-only** | Incremental polling/event path is not implemented |
| Spark MLlib and MongoDB serving | **Planned** | Depend on validated Silver/Gold datasets |

## Big Data platform

- **Steam API and catalog pages:** source data
- **Python:** discovery, qualification, historical backfill, validation
- **HDFS:** authoritative Bronze, Silver, and Gold data lake
- **PySpark:** schema enforcement, cleansing, deduplication, joins, and Parquet output
- **MapReduce:** independent game-level aggregation validated against Spark
- **Kafka and Structured Streaming:** planned incremental event transport and processing
- **Spark SQL / MLlib:** planned analytics and recommendation prediction
- **MongoDB:** planned serving collections for summaries and predictions

Local `data/raw/` directories are crawl/staging/backup areas, not the authoritative data lake.

## Immediate next checkpoint

```text
HDFS Bronze
-> PySpark Bronze-to-Silver
-> Silver validation
-> Gold base
```

The exact next engineering action is to implement and validate `src/batch/bronze_to_silver.py` with explicit schemas, validation, deduplication, type conversion, Silver Parquet output in HDFS, and Bronze-versus-Silver reconciliation.
