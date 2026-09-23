# Streaming Pipeline

## Status

This component is **Planned / Design-only**. The repository does not yet implement a poller/event producer, Kafka topic, Structured Streaming job, checkpoint, or streaming sink.

Steam is not assumed to provide a native push event stream. Near-real-time behavior is created by polling `ACTIVE` games, comparing current source state with known state, and publishing an internal event only when a change is detected.

```text
ACTIVE games
-> Steam API polling
-> change detection
-> Event Producer
-> Kafka topic: steam_events
       +-> raw event archive -> HDFS Bronze/stream_events
       +-> Spark Structured Streaming -> Silver -> incremental Gold
```

## Event contract

Every event should have an explicit envelope:

| Field | Purpose |
|---|---|
| `event_id` | Stable idempotency/deduplication identifier |
| `event_type` | `new_review`, `vote_update`, `price_update`, or `game_metadata_update` |
| `event_time` | Time the source change occurred or was observed |
| `appid` | Partitioning/routing key for a game |
| `recommendationid` | Review business key when applicable; nullable for game-only events |
| `payload` | Raw event-specific source fields plus source/version context |

The producer should also carry an observed/ingestion time and schema version. Kafka partitioning by `appid` helps retain order for changes to one game while allowing games to scale across partitions.

## Two Kafka consumption paths

1. **Raw archive consumer:** append every accepted event envelope to immutable HDFS Bronze under a `stream_events` area. This provides replay and audit history.
2. **Structured Streaming consumer:** parse the explicit schema, validate keys/types, apply event-time logic and deduplication, then merge accepted changes into compatible Silver records.

Archival must not depend on the analytical stream completing successfully. Conversely, replaying archived events must not create duplicate logical Silver/Gold records.

Only registry entries already in `ACTIVE` participate in this path. Newly qualified games first complete historical backfill, integrity validation, Bronze verification, and the transition to `ACTIVE`. Weekly discovery does not trigger full recrawls of existing `ACTIVE` games.

## Stateful processing and reliability

- **Watermark:** bounds how long late events remain eligible for stateful deduplication/aggregation. Its duration must be chosen from measured source-delay behavior.
- **Deduplication:** prefer `event_id`; use a documented business-key fallback only where required.
- **Checkpoint:** stores source offsets, state metadata, and progress outside ephemeral executor storage.
- **State:** tracks information needed for deduplication or change-aware incremental metrics; it must be bounded by event-time/watermark rules.
- **Idempotent upsert:** use stable keys such as `recommendationid` or `appid` plus a version/event timestamp so retries do not duplicate data.
- **Rejected events:** invalid envelopes should go to an auditable bad-record path rather than disappear or crash the entire stream.

## Batch/stream convergence

The streaming path must target the same logical Silver/Gold contracts as batch. A later batch rebuild from Bronze history should reproduce the same business state for the same cutoff and rules. This avoids maintaining separate “batch truth” and “streaming truth.”
