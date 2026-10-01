# Architecture

## Status Legend

- **Implemented:** verified in the current repository.
- **Planned:** future development.
- **Design-only:** architecture idea without implementation.

---

# High-Level Architecture

```text
                                Steam API
                                    |
                    +---------------+---------------+
                    |                               |
                    v                               v

             Historical Pipeline            Realtime Pipeline

                    |                               |
                    v                               v

              HDFS Bronze                    Kafka Events

                    |                               |
                    v                               v

              PySpark ETL              Spark Structured Streaming

                    |                               |
                    v                               v

              Historical Gold          Incremental Gold

                    \                               /
                     \                             /

                         Current Gold

                    /steam/gold/base_current_v1

                                |
                 +--------------+--------------+
                 |                             |
                 v                             v

          Current Analytics              Spark MLlib

                                |
                                v

                         MongoDB Serving
```

---

# Data Ownership

## HDFS

HDFS is the analytical source of truth.

Stores:

- Bronze raw data.
- Silver validated datasets.
- Gold analytical datasets.
- Streaming archives.

HDFS supports:

- Replay.
- Rebuild.
- Validation.

---

## MongoDB

MongoDB is a serving layer.

Responsibilities:

- Low-latency reads.
- API-facing views.
- Realtime materialized metrics.

MongoDB does not replace HDFS.

---

# Processing Architecture

## Historical Batch Path

Purpose:

```text
Historical dataset construction
```

Flow:

```text
Steam API

    |

Historical ingestion

    |

HDFS Bronze

    |

PySpark Silver

    |

Historical Gold
```

---

## Realtime Streaming Path

Purpose:

```text
Process newly observed reviews
```

Flow:

```text
ACTIVE Game Registry

        |

Steam Review Polling

        |

Kafka

        |

Structured Streaming

        |

Incremental Gold
```

Current event:

```text
REVIEW_CREATED
```

---

# Discovery Architecture

Discovery controls which games enter the system.

Flow:

```text
Catalog Discovery

        |

Qualification Policy

        |

Game Registry

        |

Onboarding

        |

ACTIVE Games
```

Discovery is separated from analytics processing.

---

# Current Gold Architecture

Current Gold is the convergence layer between batch and streaming.

Input:

```text
Historical Gold

        +

Incremental Gold
```

Output:

```text
/steam/gold/base_current_v1
```

Purpose:

- Latest validated analytical snapshot.
- Input for Current Analytics.
- Input for MLlib.

---

# Analytics Architecture

Historical analytics:

```text
/steam/gold/analytics/
```

Current analytics:

```text
/steam/gold/analytics_current_v1/
```

Analytics are generated from Gold datasets.

---

# ML Architecture Boundary

MLlib consumes Current Gold:

```text
Current Gold

        |

Feature Preparation

        |

Training

        |

Evaluation

        |

Versioned Model
```

Output:

```text
/steam/models/mllib/v1/<run_id>
```

ML implementation details are documented separately.

---

# Serving Architecture

Application boundary:

```text
Frontend

    |

Backend API

    |

MongoDB

    |

Prepared Analytics Views
```

The application layer should not query HDFS directly.

---

# Automation Boundary

The system has two separate automation flows.

## Discovery Automation

Responsible for:

```text
Finding and onboarding games
```

Flow:

```text
Catalog

    |

Registry

    |

ACTIVE Games
```

---

## Data Refresh Automation

Responsible for:

```text
Updating analytical freshness
```

Flow:

```text
Realtime Data

    |

Current Gold

    |

Analytics

    |

Conditional ML Retraining
```

---

# Future Architecture Extensions

Possible future additions:

- Backend API.
- Frontend dashboard.
- Production scheduler.
- Cloud deployment.
- Model serving.