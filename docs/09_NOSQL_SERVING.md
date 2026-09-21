# NoSQL Serving Design

## Status and role

MongoDB serving is **Planned**. HDFS remains the authoritative analytical lake; MongoDB provides low-latency, application-facing views derived from Gold and model outputs. MongoDB is not the system of record for raw Steam data.

## Collections

### `game_summary`

- key: `appid`
- fields: name, genres, review count, positive count/rate, average playtime, price/free indicators, source snapshot, `last_updated`
- writer: batch Gold publication and, later, idempotent incremental updates

### `genre_analytics`

- key: a normalized `genre` plus dataset/window version where needed
- fields: game count, review count, recommendation rate, average playtime, and price/engagement summaries
- writer: Spark SQL/Gold aggregation

### `model_predictions`

- key: `recommendationid` plus `model_version` if multiple model outputs must coexist
- fields: prediction, probability, model version, feature/snapshot version, and prediction time
- writer: Spark ML batch scoring and potentially a later streaming scorer

## Write behavior

Batch publication should write from a named Gold snapshot, use stable keys, and record counts before/after the write. Depending on release requirements, it may use a staging collection and atomic rename/promotion to avoid partially visible refreshes.

The planned streaming path uses `foreachBatch` or an equivalent controlled sink and key-based upserts. Replayed Kafka events or retried micro-batches must update the same logical document rather than create duplicates. Event/source versions should prevent an older update from overwriting a newer one.

## Read-back validation

Every publication should verify:

- expected collection and document counts
- uniqueness of collection keys
- sample values against the Gold source
- null/type expectations for required serving fields
- model/snapshot version metadata
- representative queries used by a dashboard/API

Serving evidence should include write results and read-back checks. Screenshots alone are supplemental; reproducible commands and machine-readable results are preferred.
