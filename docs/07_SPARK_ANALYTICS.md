# Spark Analytics Design

## Purpose

Spark Analytics generates analytical datasets from Gold data.

Responsibilities:

- Reporting.
- Visualization.
- MongoDB serving.
- Current data monitoring.

Spark Analytics does not perform:

- Raw ingestion.
- Data cleaning.
- Streaming event processing.

---

# Analytics Input

Spark Analytics supports two Gold sources.

---

## Historical Analytics

Input:

```text
/steam/gold/base
```

Purpose:

- Historical reporting.
- Baseline analysis.
- Reproducible experiments.

---

## Current Analytics

Input:

```text
/steam/gold/base_current_v1
```

Purpose:

- Latest unified analytical snapshot.
- Current reporting.
- Operational monitoring.

Output:

```text
/steam/gold/analytics_current_v1/
```

Refresh:

```bash
bash scripts/run_current_refresh.sh
```

---

# Gold Contract

Analytics expects Gold data with:

Grain:

```text
1 row = 1 recommendationid
```

Primary key:

```text
recommendationid
```

Required relationship:

```text
reviews.appid = games.appid
```

---

# Analytical Outputs

The system generates nine datasets.

---

## Game Metrics

Purpose:

```text
Recommendation performance by game
```

Contains:

- appid
- game_name
- review_count
- positive_reviews
- negative_reviews
- recommendation_rate

---

## Genre Metrics

Purpose:

```text
Recommendation trends by genre
```

Contains:

- genre
- review_count
- positive_reviews
- recommendation_rate

---

## Playtime Metrics

Purpose:

```text
Relationship between playtime and recommendation
```

Contains:

- playtime_bucket
- review_count
- recommendation_rate

---

## Free/Paid Metrics

Purpose:

```text
Compare free and paid games
```

Contains:

- game_type
- game_count
- review_count
- recommendation_rate

---

## Engagement Metrics

Purpose:

```text
Game engagement analysis
```

Contains:

- review volume
- playtime
- votes
- recommendation metrics

---

## Label Profile

Purpose:

```text
ML target distribution
```

Contains:

- positive count
- negative count
- class balance

---

## Platform Metrics

Purpose:

```text
Platform analysis
```

Contains:

- Windows
- macOS
- Linux

Platform totals may exceed review totals because games can support multiple
platforms.

---

## Category Metrics

Purpose:

```text
Steam category analysis
```

Category totals may overlap because games can have multiple categories.

---

## Purchase Metrics

Purpose:

```text
Purchase source analysis
```

Contains:

- Steam purchase.
- Other source.
- Recommendation statistics.

---

# Processing Pattern

Typical Spark flow:

```text
Gold Dataset

        |

Spark DataFrame

        |

Transformations

        |

groupBy Aggregation

        |

Parquet Output

        |

Validation
```

Processing should be deterministic from the same Gold snapshot.

---

# Validation

Analytics validation checks:

- Output row counts.
- Required fields.
- Metric ranges.
- Review reconciliation.
- Invalid values.

Example:

```text
0 <= recommendation_rate <= 1
```

Current Analytics additionally verifies reconciliation against Current Gold.

---

# Visualization

Visualization consumes analytics outputs:

```text
Gold Analytics

        |

        v

Charts / Reports
```

Visualization does not read:

- Bronze.
- Silver.
- Raw API data.

---

# MongoDB Serving

Analytics outputs can be published as serving views:

```text
Spark Analytics

        |

        v

MongoDB
```

MongoDB stores prepared views only.

HDFS remains the analytical source of truth.

---

# Future Improvements

Possible extensions:

- Incremental aggregate maintenance.
- Dashboard-specific metrics.
- Partition optimization.
- Additional business analytics.