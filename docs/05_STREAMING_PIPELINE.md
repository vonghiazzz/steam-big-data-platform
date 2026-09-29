# Streaming Pipeline V1 — New Reviews

## Status and scope

Streaming V1 is implemented for `REVIEW_CREATED` only. The implementation
has bounded unit/Spark fixture coverage and a bounded live Kafka-to-HDFS smoke
run. Price changes, game metadata changes, and review updates are deliberately
out of scope.

Steam does not provide this project with a native push stream. Near-real-time
events are created by polling only registry entries whose status is `ACTIVE`:

```text
Game Registry (ACTIVE only)
-> bounded Steam review API polling
-> change detection by recommendationid
-> Kafka topic: steam_events
       +-> raw archive -> /steam/bronze/stream_events
       +-> Spark Structured Streaming
             -> /steam/silver/reviews_incremental_v1
             -> stream-static join with /steam/silver/games
             -> /steam/gold/base_incremental_v1
```

The producer never scans the Steam catalog and never falls back to every known
game. At startup it reports registry total, `ACTIVE`, `RETIRED`, and actual poll
scope. An empty `ACTIVE` set stops cleanly. `STREAM_MAX_ACTIVE_GAMES` is a
second guard against accidental scope expansion.

## First-run bootstrap and producer state

Producer state is a local SQLite database:

```text
data/state/streaming/review_producer_v1.sqlite3
```

It is runtime state and is excluded from Git. The database stores per-game
initialization/high-water metadata, every known `recommendationid`, and a
durable event outbox.

For each newly observed `ACTIVE` game, the producer first seeds known IDs from
the canonical HDFS Bronze review file. It then polls a bounded number of the
newest Steam review pages and marks those reviews as the current baseline. This
first contact emits **zero** events, so the existing canonical 25,000 reviews
are not republished.

Later polls start from the newest page and stop when they overlap a known
`recommendationid`. If the configured page bound is exhausted without an
overlap, the game fails closed and publishes nothing. Successfully published
IDs are persisted and are not emitted again on a normal restart.

The outbox makes publishing at-least-once across a process crash. A crash after
Kafka acknowledgement but before the SQLite acknowledgement can replay the
same deterministic `event_id`; Spark therefore also deduplicates by the
business key `recommendationid`.

Production bootstrap reads `/steam/bronze/reviews` through the NameNode as the
source of truth. Local JSONL bootstrap is available only through the explicit
`--bootstrap-source local` test/fixture mode. Baseline seeding is capped by
`STREAM_BOOTSTRAP_MAX_IDS_PER_GAME` (default 1,000; the current cohort has 500
per game).

## Event contract

```json
{
  "event_id": "REVIEW_CREATED:570:123456789",
  "event_type": "REVIEW_CREATED",
  "appid": 570,
  "event_time": "2026-09-29T00:00:00+00:00",
  "produced_at": "2026-09-29T00:00:01+00:00",
  "payload": { "recommendationid": "123456789" }
}
```

`payload` is the unmodified Steam review object. Kafka messages use `appid` as
their key, retaining per-game ordering within a partition.

## Kafka local-safety configuration

`compose.streaming.yaml` defines one KRaft broker. The topic setup script
creates `steam_events` with these configurable local defaults:

| Setting | Default |
|---|---:|
| Partitions | 3 |
| Replication factor | 1 |
| `KAFKA_RETENTION_MS` | 86,400,000 (one day) |
| `KAFKA_RETENTION_BYTES` | 536,870,912 |
| Producer compression | gzip |

Kafka is bounded transport, not permanent storage. HDFS Bronze is the durable
raw event archive. Broker logs are explicitly placed in the Compose-managed
`kafka-data` volume at `/var/lib/kafka/data`; retention still applies and the
volume is not a replacement for Bronze.

```bash
docker compose -f compose.streaming.yaml up -d
bash scripts/create_streaming_topic.sh
```

## Producer cadence

Run one controlled cycle first:

```bash
bash scripts/run_review_producer.sh --once --max-games 2
```

Then run the normal producer only after bootstrap evidence is checked:

```bash
bash scripts/run_review_producer.sh
```

`STREAM_POLL_INTERVAL_SECONDS` defaults to 600 seconds. API page count, request
delay, active-game cap, retry count, and minimum host free space are also
configurable. Continuous mode also enforces the configurable
`STREAM_MIN_POLL_INTERVAL_SECONDS` safety floor (default 300 seconds). Poll
cadence is not the Spark trigger cadence: the producer asks Steam for changes,
while Spark independently checks Kafka for available events.

## Bronze archive and invalid-event policy

Every Kafka value is archived with its parsed envelope, raw value, key,
partition, offset, and Kafka timestamp under:

```text
/steam/bronze/stream_events/ingest_date=YYYY-MM-DD/
```

Business cleaning is not applied to Bronze. Malformed/unsupported records do
not terminate the stream; they are written with raw event, error reason,
available IDs, Kafka partition/offset, and processing timestamp to:

```text
/steam/quarantine/review_events/ingest_date=YYYY-MM-DD/
```

Systemic failures such as an inaccessible broker, incompatible checkpoint, or
unreadable HDFS path still fail the query visibly.

## Silver and Gold incremental outputs

Valid `REVIEW_CREATED` events reuse the canonical batch Silver Review
transformation. A static anti-join against canonical Silver IDs provides an
additional guard against reintroducing one of the historical 25,000 reviews.
Event-time watermarking and `recommendationid` deduplication bound Spark state.
Spark 4.2 uses `dropDuplicatesWithinWatermark(["recommendationid"])` after
`withWatermark("event_time_ts", "7 days")`. The state store can therefore evict
old keys as the watermark advances instead of retaining every ID forever.

Streaming does not overwrite the validated batch datasets:

```text
Batch:      /steam/silver/reviews
Streaming:  /steam/silver/reviews_incremental_v1/ingest_date=YYYY-MM-DD/

Batch:      /steam/gold/base
Streaming:  /steam/gold/base_incremental_v1/review_date=YYYY-MM-DD/
```

Gold reads only incremental Silver review files and joins them to the persisted
`/steam/silver/games` snapshot. It retains one row per recommendation and does
not introduce review text, `overall_positive_rate`, `primary_genre`, or
`package_groups`. Its `foreachBatch` sink anti-joins existing incremental Gold
IDs before append, so a deliberately reset Gold checkpoint cannot duplicate
rows already present in the output path.

## Isolated fixture/E2E resources

Fixture runs must never use the production topic, output paths, or checkpoints.
`tests/publish_streaming_smoke_fixture.py` defaults to `steam_events_test`.
Create that topic with the bounded defaults, then run its consumer with all
three isolation variables set:

```bash
KAFKA_TOPIC=steam_events_test bash scripts/create_streaming_topic.sh

KAFKA_TOPIC=steam_events_test \
STREAM_HDFS_ROOT=/steam/test/streaming_v1 \
STREAM_CHECKPOINT_ROOT=/steam/test/checkpoints \
STREAM_AVAILABLE_NOW=true \
bash scripts/run_review_streaming.sh
```

This maps test outputs below `/steam/test/streaming_v1/{bronze,silver,gold,quarantine}`
and all test checkpoints below `/steam/test/checkpoints`. Leaving the two root
variables unset keeps the production `/steam/...` paths listed above.

## Checkpoints and restart semantics

```text
/steam/checkpoints/review_bronze_archive_v1
/steam/checkpoints/review_silver_v1
/steam/checkpoints/review_quarantine_v1
/steam/checkpoints/review_gold_v1
```

These controls solve different problems:

- Kafka offsets identify transport records consumed by each Spark query.
- Spark checkpoints retain offsets, file-sink commits, watermark, and state.
- `recommendationid` is the business key used by producer and Spark deduplication.

Deleting or reusing an incompatible checkpoint is not a normal restart
procedure.

The Spark trigger defaults to one minute and can be changed with
`STREAM_SPARK_TRIGGER_INTERVAL`:

```bash
bash scripts/run_review_streaming.sh
```

For a bounded smoke run, set `STREAM_AVAILABLE_NOW=true`. Kafka-facing queries
finish first, then Gold runs against the Silver files committed in that bounded
run; no indefinite process remains.

The local Mac driver defaults to
`HDFS_DEFAULT_FS=hdfs://bda501-namenode.orb.local:8020`; override this setting
when the NameNode is exposed under a different address. `HADOOP_USER_NAME`
defaults to `hadoop` in the run script. `STREAM_HDFS_REPLICATION` defaults to
one, matching the single-DataNode lab rather than requesting unavailable
replicas.

## Disk and small-file safety

`scripts/check_streaming_storage.sh` reports host free space, Docker storage,
HDFS capacity and `/steam` usage, checkpoint sizes, and Kafka log size. It never
runs prune or deletion commands. `STREAM_MIN_FREE_GB` controls its warning/error
threshold and the same threshold causes the producer to stop before a poll.

Kafka retention and maximum offsets per trigger bound transport/backlog work.
HDFS outputs are partitioned by ingestion/review date rather than `appid`, and
each local micro-batch is coalesced to one output task. This avoids one file per
review, although low-volume minute batches can still create small files. A
future maintenance job should periodically compact many small Parquet files
into fewer larger files; V1 does not schedule destructive compaction.

Stateful shuffle partitions default to three for this single-machine lab. The
Silver transform deliberately avoids a stream-stream self-join; it reuses the
batch projection directly and derives its date partition from
`timestamp_created`. This keeps both data files and checkpoint metadata bounded
to a sensible local scale.

SQLite intentionally retains every successfully published ID for restart-safe
producer deduplication. The canonical bootstrap portion is bounded per game,
but published IDs grow over time; this is acceptable for the 50-game V1 lab and
is an explicit MVP limitation. A future version should archive an ID ledger,
prune IDs older than a proven Steam overlap/retention horizon, and run SQLite
`VACUUM` during controlled maintenance. Spark state is separately bounded by
its watermark; Kafka offsets/checkpoints do not replace business-key handling.

## New-game handoff

The preserved onboarding contract is:

```text
NEW
-> historical batch backfill
-> Bronze verification
-> producer high-water bootstrap (emits zero)
-> mark ACTIVE
-> incremental polling
```

A `NEW` game must never become a source of historical `REVIEW_CREATED` events.

## Streaming V2 backlog

- `REVIEW_UPDATED`
- `PRICE_CHANGED`
- `GAME_METADATA_CHANGED`
