# Game Discovery and Onboarding

## Purpose

Discovery defines which games may enter the managed scope. It does not clean reviews, normalize data, create ML features, or choose games based on the prediction outcome.

```text
Periodic Catalog Discovery
-> Qualification
-> Game Onboarding Policy
-> Game Registry / Watchlist
```

The repository already implements catalog probing, catalog qualification, review-availability probing, and creation of the initial selected cohort. A continuously maintained registry and scheduler are **Design-only**.

## Discovery and qualification

Periodic discovery may evaluate:

- usable and complete game metadata
- minimum review availability/volume
- API and pagination crawl feasibility
- genre, popularity, and catalog diversity
- operational exclusions such as unavailable or invalid catalog entries

Qualification is a source/scope decision. It must not use `voted_up`, recommendation rate, future model predictions, or other target-derived measures to select games; doing so would bias later analysis and ML evaluation.

## Registry / Watchlist

`data/raw/steam/selected_50_games.jsonl` is the versioned initial research cohort. It supports reproduction of the current 50-game, 25,000-review experiment and can seed a future registry. It is not a permanent architectural cap.

A registry entry should eventually track fields such as:

- `appid` and canonical game name
- lifecycle status: `NEW`, `ACTIVE`, optionally `PAUSED` or `RETIRED`
- discovery/qualification timestamps and policy version
- backfill status and last successful poll time
- latest known cursors/source versions
- failure/retry metadata and reason for state changes

The registry is control metadata; raw game/review records remain in HDFS Bronze.

## Lifecycle policy

| Status | Meaning | Processing action |
|---|---|---|
| `NEW` | Qualified but not historically onboarded | Run historical backfill once, validate, archive raw records to Bronze |
| `ACTIVE` | Backfill complete and monitored | Poll incrementally, detect changes, publish internal events |
| `PAUSED` | Temporarily excluded from polling | Preserve history and operational reason; allow controlled resume |
| `RETIRED` | No longer actively monitored | Stop polling while retaining registry and lake history |

```text
NEW -> Historical Backfill -> HDFS Bronze -> ACTIVE
ACTIVE -> Incremental API Polling -> detect changes -> Kafka
```

An existing `ACTIVE` game must not receive a full historical recrawl whenever discovery runs. Discovery updates the registry; polling handles ongoing changes. A deliberate rebuild/backfill is a separate, auditable operation.

## Current experiment versus scalable design

The current implementation is deliberately reproducible: exactly 50 selected games and 500 reviews per game. The scalable design permits new candidates in later discovery cycles without changing the meaning of that versioned experiment. New cohorts or registry versions should be recorded explicitly so EDA, ML, and benchmarks can reference a stable dataset snapshot.
