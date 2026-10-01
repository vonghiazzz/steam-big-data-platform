# Batch Pipeline

## Purpose

The batch pipeline builds the historical analytical dataset from Steam source
data.

Main flow:

```text
Steam Historical Data

        |

Historical Ingestion

        |

HDFS Bronze

        |

PySpark Silver

        |

Historical Gold

        |

Analytics / ML Snapshot
```

The batch pipeline provides reproducible historical processing.

---

# Historical Ingestion

Historical ingestion prepares source data for analytical processing.

Sources:

```text
Steam API

    |

Raw Records

    |

HDFS Bronze
```

The same flow is reused when onboarding qualified NEW games.

---

# Bronze Layer

Purpose:

```text
Raw replayable storage
```

Path:

```text
/steam/bronze
```

Contains:

- Raw game metadata.
- Historical reviews.
- Source records.
- Ingestion evidence.

Responsibilities:

- Preserve original data.
- Support replay.
- Support auditing.

Bronze does not perform analytical transformations.

---

# Silver Layer

Purpose:

```text
Cleaned and validated datasets
```

Path:

```text
/steam/silver
```

Responsibilities:

- Apply schemas.
- Normalize types.
- Validate fields.
- Remove duplicates.
- Produce analytical-ready records.

Review key:

```text
recommendationid
```

Silver provides the data contract consumed by Gold.

---

# Historical Gold Layer

Purpose:

```text
Review-level analytical dataset
```

Path:

```text
/steam/gold/base
```

Properties:

- One row per `recommendationid`.
- Joined review and game information.
- Ready for analytics and ML experiments.

Historical Gold is not modified by realtime processing.

---

# Incremental Gold Relationship

Realtime processing creates separate incremental outputs.

Path:

```text
/steam/gold/base_incremental_v1
```

Historical and incremental data are combined later by Current Gold refresh.

Flow:

```text
Historical Gold

        +

Incremental Gold

        |

        v

Current Gold
```

---

# Current Gold Refresh

Current Gold creates the latest operational snapshot.

Flow:

```text
Historical Gold

        +

Incremental Gold

        |

        v

Current Gold

        |

        +----------------+
        |                |

        v                v

Current Analytics     MLlib
```

Command:

```bash
bash scripts/run_current_refresh.sh --run-once
```

Batch processing itself does not trigger ML directly.

---

# Validation

Each batch stage validates its output.

## Bronze Validation

Checks:

- Source readability.
- Record structure.
- Required identifiers.

---

## Silver Validation

Checks:

- Schema correctness.
- Required fields.
- Duplicate handling.
- Row reconciliation.

---

## Gold Validation

Checks:

- Review grain.
- Unique recommendation IDs.
- Feature completeness.
- Analytical consistency.

---

# Rebuild Strategy

The pipeline supports controlled rebuilds.

Examples:

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

Rebuild principles:

- Keep Bronze immutable.
- Record validation evidence.
- Preserve deterministic outputs.

---

# Pipeline Responsibilities

Batch processing owns:

```text
Historical ingestion

        |

Bronze

        |

Silver

        |

Historical Gold

        |

Historical snapshots
```

Streaming processing owns:

```text
New review events

        |

Incremental datasets

        |

Current Gold refresh
```

Both pipelines share compatible data contracts.