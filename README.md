# Steam Big Data Platform

## Overview

This repository implements a local big-data platform for analysing Steam games and review behaviour. It combines a reproducible historical pipeline with an incremental review pipeline:

- HDFS stores replayable Bronze data and curated Silver/Gold data.
- PySpark DataFrames and Spark SQL perform cleaning, joining, and analytics.
- Kafka and Spark Structured Streaming process newly observed reviews.
- Hadoop Streaming MapReduce independently cross-checks key Spark metrics.
- MongoDB exposes historical aggregates and realtime materialized views to a future API.
- Matplotlib produces evidence charts from Gold Analytics.

The platform includes batch MLlib training over Gold Base. A Backend API and
frontend dashboard are not implemented yet.

## Current Status

| Component | Status | Purpose |
|---|---|---|
| Committed historical snapshot | Implemented | 50 games and 25,000 reviews for the project baseline |
| HDFS Bronze upload and verification | Implemented | Load and verify immutable raw JSONL |
| PySpark Silver | Implemented | Clean, type, validate, and deduplicate games/reviews |
| Gold Base | Implemented | Join games and reviews into one review-level analytical dataset |
| Spark Gold Analytics | Implemented | Produce nine reporting aggregates |
| Visualization | Implemented | Generate six charts from Gold Analytics |
| Hadoop Streaming MapReduce | Implemented | Independently validate recommendation metrics |
| Kafka review producer | Implemented | Poll for and publish newly observed reviews |
| Spark Structured Streaming | Implemented | Validate, deduplicate, archive, and enrich review events |
| MongoDB Historical Serving V1 | Implemented | Serve Gold Analytics snapshots |
| MongoDB Realtime Serving V2 | Implemented | Serve recent reviews and windowed realtime metrics |
| Backend API | Not implemented — next | Read-only application interface over MongoDB |
| Frontend dashboard | Not implemented — next | Visual and realtime consumer of the Backend API |
| MLlib V1 | Implemented | Batch recommendation classification over current Gold Base |

## Architecture

```text
                              Steam API
                                  |
                   +--------------+--------------+
                   |                             |
              Historical                    Incremental
                   |                             |
       committed JSONL snapshot             Review Producer
                   |                             |
                   v                             v
             HDFS Bronze                      Kafka
                   |                             |
                   v                             v
          PySpark Batch ETL            Structured Streaming
                   |                    |        |         |
                   v                    |        |         +--> MongoDB recent_reviews
                Silver                  |        |
                   |                    |        +------------> MongoDB realtime_game_metrics
                   v                    |
              Gold Base                 +---------------------> HDFS incremental data
                   |
                   +--> Spark Analytics --> Gold Analytics
                   |                           |       |
                   |                           |       +--> MongoDB historical serving
                   |                           +----------> Visualization
                   |
                   +--> Spark MLlib V1

     Bronze reviews --> Hadoop Streaming MapReduce
                              |
                              +--> cross-check Spark game_metrics
```

Historical and realtime execution are separate paths. Kafka and MongoDB use separate Compose files, while Hadoop is an external prerequisite for this repository.

## Data Layers and Source of Truth

Data ownership is deliberately separated:

- **HDFS and Kafka** provide source data, history, and replay capability.
- **HDFS Gold** is the analytical source of truth.
- **MongoDB** is a serving/materialized-view layer. It does not replace Gold or HDFS.

The medallion layers are:

| Layer | Meaning |
|---|---|
| Bronze | Raw, replayable historical records and archived stream events |
| Silver | Cleaned, typed, validated, and deduplicated data |
| Gold Base | Review-level joined dataset suitable for analytics or future ML |
| Gold Analytics | Precomputed aggregate datasets for reporting and serving |

`/steam/gold/base` contains one row per historical review. `/steam/gold/analytics/*` contains aggregates derived from that base. Historical Serving V1 copies those aggregates to MongoDB; Realtime Serving V2 maintains separate incremental views.

### Validated historical project baseline

Repository evidence confirms this project snapshot, not global Steam totals:

| Check | Expected |
|---|---:|
| Games | 50 |
| Reviews | 25,000 |
| Unique `recommendationid` | 25,000 |
| Positive reviews | 18,321 |
| Negative reviews | 6,679 |
| Gold Base grain | One row per review |

## Implemented Components

- `src/hdfs/`: Bronze upload support and integrity verification.
- `src/processing/`, `src/silver/`, `src/gold/`: historical Bronze validation, Silver cleaning, and Gold Base construction.
- `src/analytics/`: nine Spark aggregate datasets.
- `src/visualization/`: six evidence charts generated from Gold Analytics.
- `src/mapreduce/`: mapper, reducer, and comparison with Spark output.
- `src/streaming/`: the review event contract, producer state, producer, and Structured Streaming job.
- `src/serving/`: historical and realtime MongoDB writers.
- `src/discovery/`: catalog qualification and game-registry control-plane utilities.

## Repository Structure

```text
.
├── compose.mongodb.yaml       # MongoDB serving infrastructure
├── compose.streaming.yaml     # Kafka KRaft infrastructure
├── data/                      # committed project snapshot plus ignored runtime state
├── docs/                      # design and operating documentation
├── evidence/                  # validated counts, comparisons, and generated charts
├── scripts/                   # supported operational runners
├── src/
│   ├── analytics/             # Gold aggregate jobs
│   ├── gold/                  # Gold Base job
│   ├── hdfs/                  # Bronze upload and verification
│   ├── mapreduce/             # Hadoop Streaming cross-check
│   ├── serving/               # MongoDB V1/V2 writers
│   ├── silver/                # Silver jobs
│   ├── streaming/             # producer and Structured Streaming
│   └── visualization/         # chart generation
└── tests/                     # unit and Spark regression tests
```

## Prerequisites

- Git.
- Python 3.10+ and a virtual environment.
- Docker with Docker Compose. Docker Desktop, OrbStack, or another compatible runtime is acceptable.
- A running Hadoop/HDFS environment accessible to both Docker and local Spark. The scripts default to a NameNode container named `bda501-namenode`.
- A local `spark-submit`. The recorded validation used Spark 4.2.0; the default Kafka connector coordinate in the streaming runner also targets Spark 4.2.0 and Scala 2.13.
- A Java runtime compatible with the installed Spark distribution.
- Internet access when the producer calls Steam and when Spark first resolves the Kafka connector package.

The Python requirements do not install Spark/PySpark; install Spark separately. Kafka 3.9.1 and MongoDB 8.0 are defined by the repository Compose files.

> **Hadoop setup boundary:** this repository does not contain a Hadoop Compose file or a one-command Hadoop bootstrap. Start the course/lab Hadoop cluster separately before using HDFS-dependent commands.

## Quick Start

The following sections are intentionally separate. There is no supported `run_everything.sh` command.

## 1. Clone and Configure

```bash
git clone https://github.com/vonghiazzz/steam-big-data-platform.git
cd steam-big-data-platform

python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

cp .env.example .env
```

Replace every `CHANGE_ME` value in `.env`. Do not commit `.env`, credentials, or private endpoints. Common local values are:

| Variable | Typical local value or meaning |
|---|---|
| `KAFKA_HOST_PORT` | `9092` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `KAFKA_TOPIC` | `steam_events` |
| `KAFKA_TOPIC_PARTITIONS` | `3` |
| `KAFKA_RETENTION_MS` | `86400000` |
| `KAFKA_RETENTION_BYTES` | `536870912` |
| `KAFKA_LOG_SEGMENT_BYTES` | `67108864` |
| `KAFKA_COMPRESSION_TYPE` | `gzip` |
| `HADOOP_NAMENODE_CONTAINER` | `bda501-namenode`, or the actual container name |
| `HDFS_DEFAULT_FS` | An HDFS URI reachable by the process running Spark, such as `hdfs://<namenode-host>:8020` |
| `STREAM_AVAILABLE_NOW` | `false` for continuous mode; `true` for bounded processing |
| `MONGO_URI` | `mongodb://localhost:27017` |
| `MONGO_DATABASE` | `steam_analytics` |

Complete the remaining `CHANGE_ME` entries with the repository's safe local defaults unless the environment requires different limits:

```dotenv
STREAM_POLL_INTERVAL_SECONDS=600
STREAM_MIN_POLL_INTERVAL_SECONDS=300
STREAM_REQUEST_DELAY_SECONDS=1.0
STREAM_MAX_PAGES_PER_GAME=3
STREAM_MAX_ACTIVE_GAMES=100
STREAM_MIN_FREE_GB=5
STREAM_BOOTSTRAP_SOURCE=hdfs
STREAM_BOOTSTRAP_MAX_IDS_PER_GAME=1000
STREAM_SPARK_TRIGGER_INTERVAL=1 minute
STREAM_WATERMARK_DELAY=7 days
STREAM_MAX_OFFSETS_PER_TRIGGER=1000
STREAM_SPARK_SHUFFLE_PARTITIONS=3
STREAM_AVAILABLE_NOW=false
STREAM_HDFS_REPLICATION=1
```

Set `HDFS_DEFAULT_FS` separately to the reachable NameNode URI. The producer enforces its safety minimums; keep the optional fixture override variables unset outside tests.

For local batch commands, prepare a portable shell environment from the running NameNode container:

```bash
export HADOOP_NAMENODE_CONTAINER="${HADOOP_NAMENODE_CONTAINER:-bda501-namenode}"
export HDFS_DEFAULT_FS="hdfs://$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$HADOOP_NAMENODE_CONTAINER"):8020"
export HADOOP_USER_NAME="${HADOOP_USER_NAME:-hadoop}"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
```

This resolves the current container address at runtime; do not copy an address from another developer's machine.

## 2. Start Infrastructure

### Hadoop/HDFS

Start the external course/lab Hadoop environment first, then check it:

```bash
docker ps --filter "name=$HADOOP_NAMENODE_CONTAINER"
docker exec "$HADOOP_NAMENODE_CONTAINER" hdfs dfsadmin -report
```

### Kafka

```bash
docker compose -f compose.streaming.yaml up -d
bash scripts/create_streaming_topic.sh
docker compose -f compose.streaming.yaml ps
```

### MongoDB

```bash
docker compose -f compose.mongodb.yaml up -d
docker compose -f compose.mongodb.yaml ps
docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' steam-mongodb
```

Kafka and MongoDB are independent Compose projects. Starting either one does not start Hadoop or Spark.

## 3. Prepare / Verify Historical Data

The canonical historical input is committed:

- `data/raw/landing/games/games_raw.jsonl` — 50 raw game records.
- `data/raw/bronze_ready/reviews_by_game/*.jsonl` — 50 files with 500 reviews each.

Verify the local snapshot, upload it to the running HDFS cluster, then verify HDFS:

```bash
python src/hdfs/verify_bronze_integrity.py --source local
CONTAINER="$HADOOP_NAMENODE_CONTAINER" bash src/hdfs/upload_bronze.sh
python src/hdfs/verify_bronze_integrity.py \
  --source hdfs \
  --container "$HADOOP_NAMENODE_CONTAINER" \
  --out evidence/hdfs/bronze_verification.txt
```

The upload targets `/steam/bronze/games` and `/steam/bronze/reviews`. It refuses to overwrite existing Bronze data by default. Preserve that safety behaviour unless performing an intentional, controlled rebuild.

Run the Spark Bronze schema/count validation when local Spark can reach HDFS:

```bash
spark-submit --master 'local[2]' \
  --conf "spark.hadoop.fs.defaultFS=$HDFS_DEFAULT_FS" \
  --conf spark.hadoop.dfs.client.use.datanode.hostname=false \
  src/processing/bronze_ingestion.py
```

**Historical HDFS bootstrap is currently not one-command automated.** The snapshot and upload utility are present, but a fresh machine must supply and start its own compatible Hadoop environment. The repository's discovery/crawler utilities are not a guaranteed reconstruction of this exact approved snapshot.

## 4. Run Batch ETL

Produce Silver games/reviews and then Gold Base:

```bash
spark-submit --master 'local[2]' \
  --conf "spark.hadoop.fs.defaultFS=$HDFS_DEFAULT_FS" \
  --conf spark.hadoop.dfs.client.use.datanode.hostname=false \
  src/silver/silver_pipeline.py

spark-submit --master 'local[2]' \
  --conf "spark.hadoop.fs.defaultFS=$HDFS_DEFAULT_FS" \
  --conf spark.hadoop.dfs.client.use.datanode.hostname=false \
  src/gold/run_gold.py
```

A successful baseline preserves 50 games, 25,000 reviews, 25,000 Gold rows, and 25,000 unique recommendation IDs.

### Dynamic onboarding as one resumable workflow

After discovery has placed qualified games in the onboarding queue, run the
entire guarded flow with one command:

```bash
bash scripts/run_onboarding_workflow.sh \
  --batch-id onboarding-YYYYMMDD-NNN
```

For a new periodic cycle, let the runner refresh the discovery plan first and
generate the batch ID automatically:

```bash
bash scripts/run_onboarding_workflow.sh --run-discovery
```

The command above reuses the current local candidate/probe snapshot. To fetch
fresh Steam Store Search candidates before discovery, request the bounded
catalog refresh explicitly:

```bash
bash scripts/run_onboarding_workflow.sh --run-discovery --refresh-catalog
```

Catalog Refresh V1 stages the Store Search result, metadata probes, and review
probes before replacing the operational evidence. Its policy defaults are 50
results per page and at most four pages; a failed later probe leaves the prior
snapshot in place. Registry reconciliation preserves existing lifecycle states
and outstanding `QUEUED` games. The lightweight WEEKLY scheduler invokes this
bounded flow with an ACTIVE-capacity guard. An `ACTIVE` game is not demoted or
automatically re-evaluated by catalog refresh.

The runner performs prepare, historical review crawl, validation, immutable
incremental Bronze publication, Bronze verification, Silver, Gold, Analytics,
MongoDB serving, per-game readiness checks, and finally the `QUEUED` to
`ACTIVE` registry transition. Every phase is recorded under
`data/onboarding/<batch-id>/workflow_state.json`. Re-running the same batch ID
resumes completed work instead of repeating it. Registry activation occurs
only after Bronze, Silver, Gold, and MongoDB all contain the expected data.

This workflow state is also the integration contract for a future API or UI:
the UI starts a batch through a worker and reads the same state file rather
than directly editing registry or HDFS data.

## 5. Run Spark Analytics

```bash
spark-submit --master 'local[2]' \
  --conf "spark.hadoop.fs.defaultFS=$HDFS_DEFAULT_FS" \
  --conf spark.hadoop.dfs.client.use.datanode.hostname=false \
  src/analytics/run_gold_analytics.py
```

This reads `/steam/gold/base` and writes nine datasets below `/steam/gold/analytics/`:

`game_metrics`, `genre_metrics`, `playtime_metrics`, `free_paid_metrics`, `engagement_metrics`, `label_profile`, `platform_metrics`, `category_metrics`, and `purchase_metrics`.

## 6. Generate Visualizations

```bash
spark-submit --master 'local[2]' \
  --conf "spark.hadoop.fs.defaultFS=$HDFS_DEFAULT_FS" \
  --conf spark.hadoop.dfs.client.use.datanode.hostname=false \
  src/visualization/run_gold_visualization.py
```

The runner reads Gold Analytics, not Gold Base, and writes these files under `evidence/visualization/`:

- `top_games_recommendation.png`
- `genre_recommendation.png`
- `playtime_recommendation.png`
- `free_paid_recommendation.png`
- `label_distribution.png`
- `purchase_recommendation.png`

## 7. Run MapReduce Cross-check

```bash
bash scripts/run_game_recommendation_mapreduce.sh
```

The script independently recomputes per-game recommendation counts from historical Bronze with Hadoop Streaming, writes `/steam/mapreduce/game_recommendation_metrics`, and compares the result with Spark `game_metrics`.

The validated repository evidence reports 50 app IDs, 25,000 reviews, 18,321 positive, 6,679 negative, zero mismatches, and `CROSS-CHECK: PASS`. MapReduce is a foundational validation branch, not the main analytics engine. Its runner replaces only its own HDFS output path when rerun.

## 8. Load Historical Analytics into MongoDB

```bash
source scripts/load_project_env.sh .env
bash scripts/run_mongodb_serving.sh
```

This runner starts MongoDB if needed, reads all nine HDFS Gold Analytics datasets, and loads database `steam_analytics`. Documents use deterministic natural `_id` values. A reload upserts the current snapshot and removes stale snapshot documents, so rerunning is idempotent.

MongoDB is only the serving copy; `/steam/gold/analytics/*` remains the historical analytical source of truth.

## 9. Run Realtime Review Streaming

The implemented realtime event type is **`REVIEW_CREATED` only**:

```text
Steam review API
  -> Review Producer
  -> Kafka steam_events
  -> Spark Structured Streaming
  -> HDFS Bronze archive / quarantine / incremental Silver / incremental Gold
  -> MongoDB recent_reviews / realtime_game_metrics
```

Before starting the producer, confirm that `data/raw/registry/game_registry.jsonl` exists and contains eligible `ACTIVE` games. This mutable control-plane file and producer SQLite state are intentionally not committed. A fresh clone therefore needs an approved registry/onboarding step before production polling can begin.

Start Kafka and create its topic as shown in Step 2. Start MongoDB, then run both long-running processes in separate terminals.

**Terminal 1 — continuous producer (do not add `--once`):**

```bash
bash scripts/run_review_producer.sh
```

**Terminal 2 — continuous Structured Streaming with MongoDB serving:**

```bash
MONGO_REALTIME_ENABLED=true \
STREAM_AVAILABLE_NOW=false \
bash scripts/run_review_streaming.sh
```

The producer bootstraps known historical review IDs and publishes only newly observed IDs. On first contact with a game it may seed its state without emitting historical reviews.

The stream validates events, archives Kafka input, quarantines invalid records, applies a seven-day event-time watermark by default, deduplicates `recommendationid`, and anti-joins the canonical historical Silver IDs. Accepted rows are appended to incremental Silver and Gold.

MongoDB realtime semantics:

- `recent_reviews`: one document per new `recommendationid`, with a deterministic `_id`. Historical baseline reviews are excluded.
- `realtime_game_metrics`: stateful one-hour tumbling event-time windows in update mode, containing `review_count`, `positive_reviews`, `negative_reviews`, and `recommendation_rate`.

See [Continuous vs Bounded Streaming](#continuous-vs-bounded-streaming) before using smoke mode.

## 10. Verify MongoDB Serving Data

### Historical collections

```bash
docker exec steam-mongodb mongosh --quiet steam_analytics --eval \
  'db.game_metrics.countDocuments({})'

docker exec steam-mongodb mongosh --quiet steam_analytics --eval \
  'db.getCollectionNames().sort()'
```

### Realtime collections

```bash
docker exec steam-mongodb mongosh --quiet steam_analytics --eval \
  'db.recent_reviews.countDocuments({})'

docker exec steam-mongodb mongosh --quiet steam_analytics --eval \
  'db.recent_reviews.find({}).sort({timestamp_created:-1}).limit(20).forEach(printjson)'

docker exec steam-mongodb mongosh --quiet steam_analytics --eval \
  'db.realtime_game_metrics.countDocuments({})'

docker exec steam-mongodb mongosh --quiet steam_analytics --eval \
  'db.realtime_game_metrics.find({}).sort({window_start:-1}).limit(20).forEach(printjson)'
```

Realtime counts are dynamic. They depend on genuinely new reviews and completed/updated windows; do not compare them with a fixed evidence snapshot count.

## Running Tests

Run lightweight pure-Python suites from the repository root:

```bash
python -m unittest -v tests.test_gold_visualization
python -m unittest -v tests.test_game_recommendation_mapreduce
python -m unittest -v tests.test_mongodb_serving
python -m unittest -v tests.test_streaming_producer
```

Run Spark-dependent suites through the installed Spark runtime:

```bash
spark-submit --master 'local[2]' tests/test_gold_analytics.py
spark-submit --master 'local[2]' tests/test_streaming_spark.py
spark-submit --master 'local[2]' tests/test_realtime_mongodb_serving.py
```

The test suites use temporary/local fixtures unless their own setup explicitly starts an external service. They do not replace the HDFS/Kafka/Mongo runtime checks above.

## HDFS Paths

| Data | Path |
|---|---|
| Bronze games | `/steam/bronze/games` |
| Bronze historical reviews | `/steam/bronze/reviews` |
| Bronze stream archive | `/steam/bronze/stream_events` |
| Invalid review events | `/steam/quarantine/review_events` |
| Silver games | `/steam/silver/games` |
| Silver historical reviews | `/steam/silver/reviews` |
| Incremental Silver reviews | `/steam/silver/reviews_incremental_v1` |
| Gold Base | `/steam/gold/base` |
| Incremental Gold Base | `/steam/gold/base_incremental_v1` |
| Gold Analytics | `/steam/gold/analytics/<dataset>` |
| MapReduce output | `/steam/mapreduce/game_recommendation_metrics` |
| Streaming checkpoints | `/steam/checkpoints/review_*_v1` |
| MongoDB stream checkpoints | `/steam/checkpoints/mongodb/recent_reviews`, `/steam/checkpoints/mongodb/realtime_game_metrics` |

## MongoDB Collections

### Historical Serving V1

| Collection | Natural key | Purpose | Validated baseline documents |
|---|---|---|---:|
| `game_metrics` | `appid` | Review metrics by game | 50 |
| `genre_metrics` | `genre` | Metrics by genre | 17 |
| `playtime_metrics` | `playtime_bucket` | Metrics by playtime band | 5 |
| `free_paid_metrics` | `game_type` | Free versus paid comparison | 2 |
| `engagement_metrics` | `appid` | Engagement measures by game | 50 |
| `label_profile` | Fixed snapshot key | Historical label distribution | 1 |
| `platform_metrics` | `platform` | Metrics by supported platform | 3 |
| `category_metrics` | `category` | Metrics by Steam category | 59 |
| `purchase_metrics` | `purchase_source` | Steam purchase-source metrics | 2 |

### Realtime Serving V2

| Collection | Natural key | Purpose |
|---|---|---|
| `recent_reviews` | `recommendationid` | Latest incremental review feed |
| `realtime_game_metrics` | `appid` + window start/end | One-hour event-time metrics by game |

## Continuous vs Bounded Streaming

| Setting | Behaviour | Use |
|---|---|---|
| `STREAM_AVAILABLE_NOW=false` | Keeps waiting for future Kafka data | Continuous local realtime mode |
| `STREAM_AVAILABLE_NOW=true` | Processes currently available data and exits | Bounded smoke/catch-up validation |

An exited `availableNow` job is behaving as configured; it is not a continuous service. A small producer smoke run can use `bash scripts/run_review_producer.sh --once --max-games 2`, but production-style continuous polling must omit `--once`.

## Common Validation Checks

### Historical

- Games = 50.
- Reviews = 25,000.
- Positive = 18,321; negative = 6,679.
- Gold unique `recommendationid` = 25,000.
- MapReduce/Spark cross-check = PASS with zero mismatches.

### MongoDB historical

- Collection counts match the Historical Serving V1 table.
- Aggregate review totals still reconcile to 25,000.
- Re-running the loader does not create duplicate natural keys.

### Realtime

- `recent_reviews` contains only incremental recommendation IDs.
- Historical overlap is zero.
- Duplicate recommendation IDs are zero.
- Every `recommendation_rate` is in `[0, 1]`.
- Counts are dynamic; zero can be valid when Steam has no new reviews during the poll.

Evidence from the validated runs is stored under `evidence/`; it is a reproducibility reference, not a substitute for verifying a new environment.

## Run Spark MLlib V1

MLlib V1 performs a fresh batch retraining over the current dynamic
`/steam/gold/base`. The target is `voted_up`; models are a majority baseline,
Logistic Regression, and Random Forest. It is not online/incremental learning.

```bash
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"

spark-submit --master 'local[2]' \
  --conf "spark.hadoop.fs.defaultFS=$HDFS_DEFAULT_FS" \
  src/ml/run_mllib_v1.py
```

PipelineModels are written below `/steam/models/mllib/v1`, evaluation
predictions below `/steam/ml/mllib/v1/test_predictions`, and reproducibility
evidence under `evidence/ml/`. Each run records the dynamic dataset counts,
fingerprint, split seed, parameters, metrics, and output paths.

## Troubleshooting

| Symptom | Check |
|---|---|
| `.env` values fail parsing or connections | Replace every `CHANGE_ME`; keep fixture overrides unset |
| NameNode is unavailable | Start the external Hadoop environment and verify `HADOOP_NAMENODE_CONTAINER` |
| Spark cannot resolve HDFS | Set `HDFS_DEFAULT_FS` to an endpoint reachable from the process running Spark; do not reuse another machine's address |
| HDFS write is denied | Verify `HADOOP_USER_NAME`, target ownership, and permissions before changing data |
| Kafka topic is missing | Run `bash scripts/create_streaming_topic.sh` after Kafka is healthy |
| Kafka connector cannot resolve | Check internet access and that the connector coordinate matches the installed Spark/Scala build |
| MongoDB is unavailable | Check `docker compose -f compose.mongodb.yaml ps` and the `steam-mongodb` health status |
| Historical serving says PyMongo is missing | Install `requirements.txt` into the Python selected by `PYTHON_BIN` |
| Spark UI port 4040 is occupied | Spark may select another port; optionally add `--conf spark.ui.port=4041` to direct Spark commands |
| Streaming exits after catching up | Set `STREAM_AVAILABLE_NOW=false` for continuous mode |
| Producer cannot start | Provide the uncommitted ACTIVE registry and verify Steam/network access |
| No new documents appear | First bootstrap can emit zero; Steam may have no reviews newer than producer state |

## Current Limitations

- New-review creation is the only realtime event currently implemented.
- Realtime price, player-count, and game-metadata update events are not implemented.
- A fresh clone lacks both a bundled Hadoop deployment and the mutable ACTIVE producer registry.
- The Backend API and frontend dashboard are not implemented.
- MLlib V1 is batch retraining only; model serving and online learning are not
  implemented.
- Local continuous jobs stop when the host or their containers stop.
- This is a reproducible local/course platform, not production deployment infrastructure.

## Next Development Step: Backend API

The data platform's current application boundary is MongoDB. A request-serving application should not query HDFS or launch Spark for each frontend request:

```text
Frontend
   |
   v
Backend API
   |
   v
MongoDB: steam_analytics
```

Start with read-only endpoints, pagination, input validation, stable response DTOs, and clear freshness metadata. The backend framework is intentionally not selected in this repository.

## Suggested Backend API Contract

| Endpoint | MongoDB source | Intended response |
|---|---|---|
| `GET /api/health` | MongoDB ping | Service/database health |
| `GET /api/analytics/games` | `game_metrics` | Paginated game metrics |
| `GET /api/analytics/games/top` | `game_metrics` | Ranked games by a validated metric |
| `GET /api/analytics/genres` | `genre_metrics` | Genre aggregates |
| `GET /api/analytics/playtime` | `playtime_metrics` | Playtime-band aggregates |
| `GET /api/analytics/free-paid` | `free_paid_metrics` | Free/paid comparison |
| `GET /api/analytics/platforms` | `platform_metrics` | Platform aggregates |
| `GET /api/analytics/categories` | `category_metrics` | Category aggregates |
| `GET /api/analytics/purchase` | `purchase_metrics` | Purchase-source aggregates |
| `GET /api/realtime/reviews` | `recent_reviews` | Latest incremental reviews |
| `GET /api/realtime/reviews?appid=<appid>` | `recent_reviews` | Latest reviews for one game |
| `GET /api/realtime/games` | `realtime_game_metrics` | Latest per-game windows |
| `GET /api/realtime/games/<appid>` | `realtime_game_metrics` | Windows for one game |

Frontend development should follow the API. Suggested dashboard views include top games by recommendation rate, genre recommendation, playtime versus recommendation, free versus paid comparison, positive/negative distribution, a recent-review feed, and one-hour realtime recommendation metrics.

## Development Principles

- Keep Bronze immutable and replayable.
- Treat HDFS Gold as analytical truth and MongoDB as a rebuildable serving view.
- Preserve one deterministic natural key for every MongoDB document.
- Keep historical and realtime paths explicit; do not mix their counts.
- Make streaming jobs restartable through durable checkpoints and idempotent sinks.
- Validate aggregates with an independent path where practical.
- Keep secrets, private endpoints, mutable registries, and runtime state out of Git.
- Document only commands and features that the repository actually supports.

## Project Status Summary

The repository has a validated historical path from committed JSONL through HDFS Bronze, PySpark Silver/Gold, nine analytics datasets, visualizations, MapReduce cross-validation, and MongoDB Historical Serving V1. It also has an incremental path from Steam review polling through Kafka and Structured Streaming to HDFS incremental layers and MongoDB Realtime Serving V2.

To reproduce the system on a new machine, supply a compatible Hadoop environment and, for live polling, an approved ACTIVE game registry. The next product-development slice is a read-only Backend API over MongoDB, followed by the frontend dashboard.
