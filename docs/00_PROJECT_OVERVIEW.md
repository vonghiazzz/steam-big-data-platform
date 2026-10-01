# Project Overview

## Goal

This BDA501 final project builds a reproducible Big Data platform for Steam
game and review analytics.

The main research question:

> Can player behavior and game characteristics be used to predict whether a
> Steam user recommends a game?

Prediction target:

```text
voted_up
```

The ML pipeline uses behavioral and game metadata features.
Review text is outside the primary experiment.

---

# System Summary

The platform contains four major areas:

```text
Data Ingestion

        |

Data Lake Processing

        |

Analytics and Machine Learning

        |

Serving Layer
```

Main technologies:

| Component | Purpose |
|---|---|
| HDFS | Authoritative data lake storage |
| PySpark | Batch processing, cleaning, analytics |
| Kafka | Realtime event transport |
| Structured Streaming | Incremental review processing |
| Hadoop MapReduce | Independent Spark validation |
| MongoDB | Serving/materialized views |
| Spark MLlib | Recommendation classification |

---

# Current Status

| Area | Status |
|---|---|
| Historical ingestion | Implemented |
| HDFS Bronze/Silver/Gold | Implemented |
| Game Discovery Control Plane | Implemented |
| Dynamic game onboarding | Implemented |
| Kafka review streaming | Implemented |
| Spark Structured Streaming | Implemented |
| MongoDB Historical Serving | Implemented |
| MongoDB Realtime Serving | Implemented |
| Spark Analytics | Implemented |
| Current Gold refresh | Implemented |
| Current Analytics refresh | Implemented |
| MLlib V1 | Implemented |
| Current Refresh Scheduler | Implemented |
| Backend API | Not implemented |
| Frontend dashboard | Not implemented |

---

# Dataset Snapshot

The original research snapshot:

```text
50 games

25,000 historical reviews

18,321 positive reviews

6,679 negative reviews
```

This snapshot provides reproducible historical validation.

It is not a permanent system limitation.

The scalable system supports future game onboarding through the Discovery
Control Plane.

---

# Main Data Paths

Historical data:

```text
Steam API

    |

HDFS Bronze

    |

Silver

    |

Historical Gold
```

Realtime data:

```text
ACTIVE Games

    |

Steam Review Polling

    |

Kafka

    |

Structured Streaming

    |

Incremental Gold
```

The two paths converge into:

```text
Current Gold
```

which is used by:

- Current Analytics
- MLlib training

---

# Automation

The platform contains two independent automation flows.

## Discovery Scheduler

Purpose:

```text
Discover and onboard games
```

Command:

```bash
bash scripts/run_discovery_scheduler.sh
```

---

## Current Refresh Scheduler

Purpose:

```text
Refresh Current Gold

        |

Refresh Analytics

        |

Conditional ML Retraining
```

Command:

```bash
bash scripts/run_current_refresh.sh
```

---

# Current Limitations

The following application layers are not implemented:

- Backend API.
- Frontend dashboard.
- Model serving API.

The current repository focuses on the Big Data processing platform and
reproducible analytics workflow.

---

# Next Development Step

The next application boundary is:

```text
Frontend

    |

Backend API

    |

MongoDB Serving Layer
```

The API should consume prepared MongoDB views instead of querying HDFS or
executing Spark jobs directly.