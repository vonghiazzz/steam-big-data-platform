# Deployment and Operations

## Current and planned platform

HDFS Bronze storage/verification is **Implemented**. Spark batch processing is the next checkpoint. Kafka, Structured Streaming, MongoDB serving, and broader cluster deployment remain **Planned / Design-only** unless later evidence proves otherwise.

## Core services

- **HDFS:** authoritative Bronze/Silver/Gold storage, replication, immutable raw history, Parquet outputs
- **Spark:** driver builds the execution plan; executors process partitions and shuffle data
- **Kafka:** planned `steam_events` transport, partitioned primarily by `appid`, with retention for consumer recovery
- **MongoDB:** planned serving views, not raw source-of-truth storage

Spark can run under Standalone, YARN, or Kubernetes depending on the final environment. The project should record the actual chosen deployment and software versions rather than claim all options were tested.

## Checkpoints and recovery

Structured Streaming checkpoints must live on durable shared storage and contain source progress, offsets, state metadata, and commit information. A job restart should use the same compatible query/checkpoint identity. State must be bounded through watermarks or explicit expiry.

Recovery assumptions:

- HDFS Bronze allows Silver/Gold batch rebuilds.
- Kafka retention allows streaming consumers to resume within the retention window.
- Failed Spark tasks can be retried from lineage/input.
- Idempotent sinks prevent task or micro-batch retries from duplicating logical results.

Checkpoint backup is not a substitute for the immutable Bronze event archive.

## Monitoring

Operational evidence should include:

- HDFS capacity, live nodes, replication/under-replicated blocks
- Spark UI stages, task failures, executor memory, skew, shuffle read/write
- driver and executor logs/metrics
- Kafka broker/topic health and consumer lag
- Structured Streaming input rate, processing rate, batch duration, watermark, and state size
- MongoDB write errors, latency, and read-back checks
- pipeline reconciliation counts and freshness timestamps

Alerts should distinguish data-quality failures from infrastructure failures.

## Scalability considerations

- Increase HDFS/Spark partitions with measured data volume; avoid many tiny files.
- Scale Kafka partitions and consumers while preserving per-`appid` ordering needs.
- Broadcast the small game dimension only after confirming its size.
- Detect hot `appid` keys that can cause shuffle or Kafka partition skew.
- Use compaction/partitioning policies for Bronze event archives and Silver/Gold Parquet.
- Treat the current 50-game experiment as correctness evidence, not production-scale performance evidence.

Secrets must remain outside Git. Deployment changes should be reproducible, reviewed, and accompanied by rollback/recovery instructions.
