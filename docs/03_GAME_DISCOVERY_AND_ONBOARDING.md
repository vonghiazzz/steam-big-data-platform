# Game Discovery and Onboarding

## Purpose

The Discovery and Control Plane manages the lifecycle of Steam games entering
the processing scope.

Discovery answers:

> Which games should be processed?

It does not perform:

- Review cleaning.
- Feature engineering.
- Analytics aggregation.
- ML target filtering.

---

# Discovery Flow

```text
Steam Catalog Discovery

        |

Qualification Policy

        |

Game Registry / Watchlist

        |

Onboarding Workflow

        |

ACTIVE Games
```

---

# Discovery Components

| Component | Status |
|---|---|
| Steam catalog probing | Implemented |
| Metadata qualification | Implemented |
| Review availability checking | Implemented |
| Policy evaluation | Implemented |
| Game Registry | Implemented |
| Registry reconciliation | Implemented |
| Catalog refresh | Implemented |
| Discovery Scheduler | Implemented |
| Dynamic onboarding workflow | Implemented |

Production registry database and administration UI are future work.

---

# Catalog Refresh

Catalog refresh discovers candidate games.

Flow:

```text
Steam Store Search

        |

Candidate AppIDs

        |

Metadata Probe

        |

Review Probe

        |

Qualification Policy

        |

Registry Update
```

Refresh is bounded and does not attempt to mirror the complete Steam catalog.

Command:

```bash
bash scripts/run_onboarding_workflow.sh \
  --run-discovery \
  --refresh-catalog
```

Failed refreshes do not replace the previous valid snapshot.

---

# Qualification Policy

Qualification determines whether a game is eligible.

Typical rules:

| Rule | Purpose |
|---|---|
| App type | Accept valid games |
| Release age | Avoid very new games |
| Minimum reviews | Ensure enough review data |
| Metadata availability | Ensure processing readiness |
| Review endpoint availability | Ensure crawl feasibility |

Qualification does not use:

- `voted_up`
- Recommendation rate
- Positive/negative ratio

because these are analytical outcomes.

---

# Registry Lifecycle

Registry tracks game processing state.

Lifecycle:

```text
DISCOVERED

      |

QUALIFIED

      |

QUEUED

      |

BACKFILLING

      |

ACTIVE
```

Failure states:

```text
BACKFILLING

      |

FAILED

      |

RETRY
```

Optional states:

```text
PAUSED

RETIRED
```

---

# NEW Game Onboarding

A NEW game receives historical processing:

```text
NEW

 |

Historical Backfill

 |

Bronze Validation

 |

Silver / Gold Processing

 |

ACTIVE
```

Historical backfill happens once.

---

# ACTIVE Game Processing

ACTIVE games move to incremental processing:

```text
ACTIVE Game

        |

Steam Review Polling

        |

Change Detection

        |

Kafka Event

        |

Structured Streaming
```

ACTIVE games are not fully crawled during normal discovery cycles.

A complete rebuild is an explicit operation.

---

# Discovery Scheduler

The scheduler automates periodic discovery.

Commands:

## Dry Run

```bash
bash scripts/run_discovery_scheduler.sh \
  --run-once \
  --dry-run
```

## Run Once

```bash
bash scripts/run_discovery_scheduler.sh \
  --run-once
```

## Continuous Mode

```bash
bash scripts/run_discovery_scheduler.sh
```

Scheduler maintains:

- Execution state.
- Locking.
- Discovery period.
- Unfinished onboarding batches.

---

# Capacity Management

Discovery respects ACTIVE capacity.

Flow:

```text
Maximum Capacity

        |

Current ACTIVE Games

        |

Available Slots
```

Existing QUEUED games reserve capacity first.

When capacity is full:

```text
Discovery continues

NEW games remain NEW

Onboarding waits
```

---

# Relationship With Data Pipeline

Discovery controls scope.

The data pipeline processes approved games:

```text
Discovery

    |

Registry

    |

ACTIVE Games

    |

Streaming Producer

    |

Incremental Gold

    |

Current Gold Refresh

    |

Analytics / ML
```

Discovery and Current Refresh are separate automation flows.

Discovery controls:

```text
Which games enter the system?
```

Current Refresh controls:

```text
When should analytics and ML update?
```