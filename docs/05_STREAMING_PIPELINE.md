# Streaming Pipeline

## Purpose

Streaming V1 processes newly observed Steam reviews in near realtime.

Supported event:

```text
REVIEW_CREATED
```

Main flow:

```text
ACTIVE Game Registry

        |

Steam Review Polling

        |

Kafka

        |

Spark Structured Streaming

        |

Incremental Silver

        |

Incremental Gold

        |

Realtime Serving
```

---

# Event Source

Steam does not provide a native push stream for this project.

Realtime events are generated through polling:

```text
Game Registry

        |

ACTIVE Games

        |

Steam Review API

        |

Change Detection

        |

Kafka Event
```

The producer only processes managed ACTIVE games.

It does not crawl the complete Steam catalog.

---

# Kafka Layer

Kafka provides temporary event transport.

Topic:

```text
steam_events
```

Purpose:

- Decouple producer and streaming jobs.
- Provide event buffering.
- Support consumer recovery.

Kafka is not the source of truth.

Durable storage remains HDFS.

---

# Event Contract

Main event:

```text
REVIEW_CREATED
```

Example:

```json
{
  "event_type": "REVIEW_CREATED",
  "appid": 570,
  "recommendationid": "123456789",
  "event_time": "2026-09-30T12:49:46Z"
}
```

Primary business key:

```text
recommendationid
```

Used for:

- Deduplication.
- Idempotent processing.

---

# Producer State

The producer maintains runtime state:

```text
data/state/streaming/
```

State includes:

- Known recommendation IDs.
- Per-game polling state.
- Event delivery information.

Runtime state is not committed to Git.

---

# Bootstrap Behavior

When a game becomes ACTIVE:

```text
ACTIVE Game

        |

Seed historical review IDs

        |

Start polling

        |

Emit only new reviews
```

Historical reviews are not replayed as realtime events.

---

# Structured Streaming Flow

Spark Structured Streaming consumes Kafka events:

```text
Kafka

 |

Event Validation

 |

Deduplication

 |

Incremental Silver

 |

Incremental Gold
```

Processing guarantees:

- Invalid events are rejected.
- Duplicate recommendation IDs are removed.
- Checkpoints support restart.

---

# Incremental Silver

Purpose:

```text
Validated realtime records
```

Path:

```text
/steam/silver/reviews_incremental_v1
```

Responsibilities:

- Validate event schema.
- Normalize fields.
- Preserve streaming contract.

---

# Incremental Gold

Purpose:

```text
Realtime analytical records
```

Path:

```text
/steam/gold/base_incremental_v1
```

Responsibilities:

- Join game metadata.
- Produce review-level records.
- Maintain recommendation grain.

Incremental Gold does not overwrite Historical Gold.

---

# Current Gold Integration

Streaming output becomes part of the operational snapshot through Current Gold.

Flow:

```text
Historical Gold

        +

Incremental Gold

        |

        v

Current Gold

        |

        v

Current Analytics / ML Refresh
```

Streaming does not directly trigger ML training.

Current Refresh Scheduler controls analytics and ML updates.

---

# Realtime Serving

Realtime outputs can be served through MongoDB.

Collections:

```text
recent_reviews

realtime_game_metrics
```

Purpose:

- Latest review feed.
- Windowed game metrics.

---

# Checkpoints

Streaming checkpoints store:

- Kafka offsets.
- Spark progress.
- Stateful processing information.

Example:

```text
/steam/checkpoints/
```

Checkpoint deletion should only happen during controlled rebuilds.

---

# Running Streaming

Start Kafka:

```bash
docker compose -f compose.streaming.yaml up -d
```

Create topic:

```bash
bash scripts/create_streaming_topic.sh
```

Run producer:

```bash
bash scripts/run_review_producer.sh
```

Run streaming:

```bash
bash scripts/run_review_streaming.sh
```

---

# Validation

Streaming validation includes:

- Event schema validation.
- Duplicate detection.
- Incremental output checks.
- MongoDB serving checks.

Expected properties:

```text
No duplicate recommendationid

Valid recommendation_rate

Correct incremental counts
```

---

# Current Limitations

Not implemented:

- REVIEW_UPDATED.
- PRICE_CHANGED.
- GAME_METADATA_CHANGED.
- Player-count events.

Future realtime events should follow the same event → Silver → Gold contract.