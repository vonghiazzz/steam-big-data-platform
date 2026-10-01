# NoSQL Serving Design

## Purpose

MongoDB provides an application-facing serving layer.

MongoDB is not the analytical source of truth.

Architecture boundary:

```text
HDFS Gold

      |

      v

Spark Analytics

      |

      v

MongoDB Serving

      |

      v

Backend API / Dashboard
```

Responsibilities:

- Low-latency reads.
- Prepared analytical views.
- Realtime materialized metrics.

HDFS remains the authoritative data lake.

---

# Historical Serving

Historical serving publishes Spark Analytics outputs.

Source:

```text
/steam/gold/analytics/
```

Collections:

```text
game_metrics

genre_metrics

playtime_metrics

free_paid_metrics

engagement_metrics

label_profile

platform_metrics

category_metrics

purchase_metrics
```

Purpose:

- Dashboard queries.
- API responses.
- Analytical summaries.

---

# Realtime Serving

Realtime serving is generated from Structured Streaming.

## recent_reviews

Purpose:

```text
Latest incremental review feed
```

Key:

```text
recommendationid
```

Contains:

- appid
- voted_up
- playtime information
- timestamps
- ingestion metadata

---

## realtime_game_metrics

Purpose:

```text
Realtime recommendation metrics
```

Key:

```text
appid + window_start + window_end
```

Contains:

- review_count
- positive_reviews
- negative_reviews
- recommendation_rate

---

# Current Analytics Serving

Current Analytics is generated from:

```text
/steam/gold/base_current_v1
```

Output:

```text
/steam/gold/analytics_current_v1/
```

When application requirements need the latest unified snapshot, these datasets
can be published to MongoDB.

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

Current Analytics

        |

        v

MongoDB Serving
```

---

# Write Strategy

MongoDB writes should:

- Use deterministic keys.
- Be idempotent.
- Preserve dataset/version metadata.
- Validate results after writing.

Recommended flow:

```text
Spark Output

      |

Validation

      |

MongoDB Upsert

      |

Read-back Check
```

---

# Validation

Every publication should verify:

## Collection

- Exists.
- Expected document count.

## Data Quality

- Unique keys.
- Required fields.
- Correct data types.
- Valid metric ranges.

Example:

```text
0 <= recommendation_rate <= 1
```

---

# Version and Freshness

Serving documents should include:

- Source snapshot.
- Generated timestamp.
- Dataset version.
- Model version when applicable.

This allows consumers to understand data freshness.

---

# API Boundary

Future API should read MongoDB.

Flow:

```text
Frontend

    |

Backend API

    |

MongoDB Serving
```

The API should not:

- Query raw HDFS.
- Launch Spark jobs per request.
- Recalculate analytics dynamically.

---

# Current Limitations

Not implemented:

- Backend API.
- Frontend dashboard.
- Model prediction serving.
- Production MongoDB deployment.

---

# Future Extensions

Possible improvements:

- API caching.
- Dashboard-specific collections.
- Prediction serving.
- Data freshness monitoring.
- Automated serving refresh.