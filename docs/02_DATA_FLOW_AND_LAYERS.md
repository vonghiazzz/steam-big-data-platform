# Data Flow and Lake Layers

## Overview

The platform follows a medallion-style data architecture:

```text
Raw Sources

    |

    v

Bronze

    |

    v

Silver

    |

    v

Gold

    |

    +----------------+
    |                |
    v                v

Analytics          MLlib
```

Historical and realtime pipelines use compatible data contracts and converge at
Current Gold.

---

# Data Storage Layers

## Bronze

Purpose:

```text
Raw replayable storage
```

Path:

```text
/steam/bronze
```

Contains:

- Raw Steam game metadata.
- Raw historical reviews.
- Streaming event archives.

Properties:

- Immutable.
- Replayable.
- Minimal transformation.

Bronze is not analytical data.

---

# Silver

Purpose:

```text
Validated and cleaned datasets
```

Path:

```text
/steam/silver
```

Responsibilities:

- Apply explicit schemas.
- Normalize data types.
- Validate required fields.
- Remove duplicates.
- Preserve rejected records.

Review primary key:

```text
recommendationid
```

Silver provides the contract used by Gold processing.

---

# Gold

Gold contains analytical-ready datasets.

There are two main Gold paths.

---

## Historical Gold

Path:

```text
/steam/gold/base
```

Purpose:

- Historical analytics.
- Baseline experiments.
- Reproducible snapshots.

Grain:

```text
1 row = 1 recommendationid
```

---

## Incremental Gold

Path:

```text
/steam/gold/base_incremental_v1
```

Purpose:

- Store newly observed realtime reviews.
- Preserve streaming output separately.

Incremental Gold does not overwrite Historical Gold.

---

# Current Gold

Current Gold is the unified operational snapshot.

Flow:

```text
Historical Gold

        +

Incremental Gold

        |

        v

Current Gold
```

Path:

```text
/steam/gold/base_current_v1
```

Properties:

- Same schema contract as Gold.
- One row per `recommendationid`.
- Deduplicated before writing.
- Used by Analytics and MLlib.

Current Gold allows the platform to use the latest validated data without
changing historical or incremental sources.

---

# Analytics Layer

Analytics consumes Gold datasets.

## Historical Analytics

Input:

```text
/steam/gold/base
```

Output:

```text
/steam/gold/analytics/<dataset>
```

Purpose:

- Historical reporting.
- Baseline visualization.
- Historical serving.

---

## Current Analytics

Input:

```text
/steam/gold/base_current_v1
```

Output:

```text
/steam/gold/analytics_current_v1/<dataset>
```

Purpose:

- Latest reporting.
- Current dashboard data.
- Current monitoring.

---

# ML Data Flow

MLlib consumes Current Gold.

Flow:

```text
Current Gold

      |

Feature Preparation

      |

Training Dataset

      |

Model Training

      |

Versioned Output
```

Input:

```text
/steam/gold/base_current_v1
```

Output:

```text
/steam/models/mllib/v1/<run_id>
```

---

# Realtime Data Flow

Realtime reviews enter through Streaming.

```text
ACTIVE Game

      |

Steam Polling

      |

Kafka

      |

Structured Streaming

      |

Incremental Silver

      |

Incremental Gold

      |

Current Gold Refresh
```

Realtime processing does not modify Historical Gold.

---

# Validation Boundaries

Each layer validates its own contract.

## Bronze Validation

Checks:

- Source readability.
- Record structure.
- Required identifiers.

---

## Silver Validation

Checks:

- Schema compatibility.
- Required fields.
- Duplicate handling.
- Data types.

---

## Gold Validation

Checks:

- Review grain.
- Unique recommendation IDs.
- Feature completeness.
- Analytical consistency.

---

## Current Gold Validation

Checks:

- Historical + incremental compatibility.
- Deduplication.
- Final row count.
- Unique recommendation IDs.

---

# Rebuild Strategy

The platform supports controlled rebuilds.

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

Analytics / ML refresh
```

Bronze remains unchanged during rebuilds.

This allows reproducible processing from authoritative source data.