# Deployment and Operations

## Purpose

This document describes how to operate the local Big Data platform.

The platform includes:

- HDFS data lake.
- Spark batch processing.
- Kafka realtime transport.
- Spark Structured Streaming.
- MongoDB serving.
- Discovery automation.
- Current Refresh automation.
- MLlib batch retraining.

---

# Service Responsibilities

## HDFS

Role:

```text
Analytical source of truth
```

Stores:

- Bronze datasets.
- Silver datasets.
- Gold datasets.
- Streaming archives.

---

## Spark

Responsibilities:

- Batch ETL.
- Analytics.
- Structured Streaming.
- MLlib training.

---

## Kafka

Role:

```text
Realtime event transport
```

Current event:

```text
REVIEW_CREATED
```

Kafka is not permanent storage.

---

## MongoDB

Role:

```text
Serving layer
```

Used for:

- Historical analytics views.
- Realtime metrics.
- Future API access.

---

# Scheduler Operations

The platform has two independent schedulers.

---

# Discovery Scheduler

Purpose:

```text
Discover and onboard games
```

Flow:

```text
Catalog Discovery

        |

Qualification

        |

Registry

        |

Onboarding

        |

ACTIVE Games
```

Run:

```bash
bash scripts/run_discovery_scheduler.sh
```

Run one cycle:

```bash
bash scripts/run_discovery_scheduler.sh --run-once
```

Dry run:

```bash
bash scripts/run_discovery_scheduler.sh \
  --run-once \
  --dry-run
```

Runtime state:

```text
data/state/discovery/
```

---

# Current Refresh Scheduler

Purpose:

```text
Refresh Current Gold

        |

Refresh Analytics

        |

Conditional ML Retraining
```

Run:

```bash
bash scripts/run_current_refresh.sh
```

Run one cycle:

```bash
bash scripts/run_current_refresh.sh --run-once
```

Dry run:

```bash
bash scripts/run_current_refresh.sh \
  --run-once \
  --dry-run
```

Runtime state:

```text
data/state/current_refresh/
```

Tracked information:

- Last refresh time.
- Current Gold row count.
- Last analytics refresh.
- Last ML run.
- Dataset fingerprint.
- Retraining decision.

---

# Runtime State

Runtime state is not committed to Git.

Examples:

```text
data/state/

    discovery/

    current_refresh/

    streaming/
```

Contains:

- Scheduler state.
- Locks.
- Producer state.
- Checkpoint metadata.

---

# Recovery Strategy

The platform supports rebuilding from authoritative layers.

Recovery flow:

```text
Bronze

 |

Silver rebuild

 |

Gold rebuild

 |

Current Gold rebuild

 |

Analytics refresh
```

Bronze remains unchanged during rebuilds.

---

# Streaming Operations

Start Kafka:

```bash
docker compose -f compose.streaming.yaml up -d
```

Create topic:

```bash
bash scripts/create_streaming_topic.sh
```

Start producer:

```bash
bash scripts/run_review_producer.sh
```

Start streaming:

```bash
bash scripts/run_review_streaming.sh
```

---

# Data Refresh Operations

Refresh current snapshot:

```bash
bash scripts/run_current_refresh.sh --run-once
```

The workflow:

```text
Incremental Data

        |

Current Gold

        |

Current Analytics

        |

ML Retraining Policy
```

---

# Monitoring

## HDFS

Monitor:

- Storage capacity.
- Data availability.
- Permissions.
- Replication status.

---

## Spark

Monitor:

- Job status.
- Stage failures.
- Executor memory.
- Shuffle performance.
- Runtime.

---

## Kafka

Monitor:

- Broker status.
- Topic availability.
- Consumer lag.

---

## Streaming

Monitor:

- Input rate.
- Processing rate.
- Batch duration.
- Watermark.
- State size.

---

## MongoDB

Monitor:

- Write errors.
- Collection counts.
- Read-back validation.
- Query latency.

---

# Environment Configuration

Secrets remain outside Git.

Required configuration:

```text
.env
```

Important variables:

```text
HDFS_DEFAULT_FS

HADOOP_USER_NAME

KAFKA_BOOTSTRAP_SERVERS

MONGO_URI
```

---

# Local Execution Order

Recommended order:

## 1. Start Hadoop/HDFS

Verify:

```bash
hdfs dfsadmin -report
```

---

## 2. Start Kafka

```bash
docker compose -f compose.streaming.yaml up -d
```

---

## 3. Start MongoDB

```bash
docker compose -f compose.mongodb.yaml up -d
```

---

## 4. Start Streaming

Producer:

```bash
bash scripts/run_review_producer.sh
```

Streaming:

```bash
bash scripts/run_review_streaming.sh
```

---

## 5. Refresh Current Data

```bash
bash scripts/run_current_refresh.sh --run-once
```

---

# Current Limitations

Not included:

- Kubernetes deployment.
- Cloud infrastructure.
- Production orchestration.
- External service supervisor.
- Backend deployment.
- Frontend deployment.

---

# Future Operations

Possible improvements:

- OS/service supervisor.
- Container orchestration.
- Centralized monitoring.
- Alerting.
- Production deployment pipeline.