# Game Discovery and Onboarding

## Purpose and status

The Discovery & Control Plane keeps the existing architecture unchanged:

```text
Periodic Catalog Discovery
-> Qualification
-> Game Onboarding Policy
-> Game Registry / Watchlist
```

Catalog probing, the initial research selection, the versioned policy configuration/evaluator, the local JSONL Discovery Control Plane V1, and the lightweight WEEKLY scheduler are **Implemented**. A production registry service and admin interface remain **Design-only**.

## Bounded fresh catalog refresh

Catalog Refresh V1 is an explicit network operation:

```text
Steam Store Search
-> bounded candidate AppIDs
-> metadata probe
-> review-availability probe
-> existing qualification policy
-> registry and onboarding queue reconciliation
```

Run the complete onboarding workflow with a fresh catalog window using:

```bash
bash scripts/run_onboarding_workflow.sh --run-discovery --refresh-catalog
```

Without `--refresh-catalog`, discovery continues to use the existing local
probe snapshot. The configured default refresh is bounded to 50 results per
page and four pages; it never attempts to mirror the complete Steam catalog.
All probe output is produced and validated in staging first. Candidate,
metadata, eligible-game, and review-probe evidence replaces the operational
snapshot only after every stage completes consistently, so a later-stage
failure leaves the previous snapshot usable.

## WEEKLY discovery scheduler

Run one safe planning cycle without Steam or onboarding writes:

```bash
bash scripts/run_discovery_scheduler.sh --run-once --dry-run
```

Run one due cycle with the configured workflow:

```bash
bash scripts/run_discovery_scheduler.sh --run-once
```

Without `--run-once`, the process periodically checks whether the current UTC
ISO week has already completed. State is stored in
`data/state/discovery/scheduler_v1.json`, and a non-blocking scheduler lock
prevents overlapping scheduler runs. The onboarding workflow retains its own
compatible `.workflow.lock` and batch state; a failed or paused batch is
resumed by batch ID before any conflicting batch can start.

The scheduled flow is:

```text
WEEKLY scheduler
-> rotating catalog refresh
-> qualification policy
-> registry reconciliation
-> ACTIVE capacity guard
-> resumable onboarding
-> ACTIVE
```

Capacity is calculated as
`min(max_new_games_per_cycle, max_active_games - active_count)`, bounded at
zero. Existing `QUEUED` games reserve slots before additional `NEW` games are
queued. At capacity zero, discovery may still refresh and reconcile candidates,
but NEW games remain NEW and onboarding is skipped safely.

Scheduler automation requires this scheduler process to be running. OS boot
auto-start and external process supervision remain separate operational
concerns.

## Qualification versus onboarding

Qualification answers **“Is this game eligible?”** It is a source/scope decision, not review cleaning, feature engineering, or target optimization. The default rules are loaded from `config/discovery_policy.json`:

| Qualification rule | Default | Failure reason |
|---|---:|---|
| Steam application type | `game` | `INVALID_APP_TYPE` |
| Minimum release age | 30 days | `GAME_TOO_NEW` |
| Minimum available reviews | 1,000 | `INSUFFICIENT_REVIEWS` |
| Metadata available | required | `METADATA_UNAVAILABLE` |
| Review endpoint available | required | `REVIEW_ENDPOINT_UNAVAILABLE` |

A missing or invalid release date is explicitly rejected as `INVALID_RELEASE_DATE`; it is never treated as old enough. Release age is calculated as `current_date - release_date`, and the boundary is inclusive: a game exactly 30 days old passes the default age rule.

The optional `min_playtime_minutes` setting is `null`, meaning **disabled**. A playtime-above-two-hours rule is not enabled because playtime is an important EDA/ML feature. Likewise, `voted_up`, recommendation rate, and positive/negative ratio are not qualification rules; filtering on the analytical target would introduce selection bias.

Onboarding answers **“If eligible, should/how should it be onboarded?”** Its current operational defaults are:

| Policy | Default |
|---|---:|
| Historical target | 500 reviews/game |
| Maximum new games per discovery cycle | 10 |
| Catalog refresh page size | 50 |
| Maximum catalog pages per refresh | 4 |
| Discovery frequency | `WEEKLY` |
| Maximum retry attempts | 3 |

The values are externalized so later runs or an administration interface can change them without rewriting the evaluator.

The two review thresholds have different meanings. **1,000 available reviews** is the game-level eligibility threshold. **500 reviews/game** is the historical sampling/backfill target for the current reproducible research snapshot.

## Explainable qualification result

Qualification returns every failed rule, not only a Boolean. For example:

```json
{
  "appid": 123456,
  "qualified": false,
  "policy_version": 1,
  "release_age_days": 12,
  "reasons": [
    {
      "rule": "GAME_TOO_NEW",
      "expected": 30,
      "actual": 12
    },
    {
      "rule": "INSUFFICIENT_REVIEWS",
      "expected": 1000,
      "actual": 320
    }
  ]
}
```

`src/discovery/policy.py` contains the standard-library configuration model, validation, date parsing, and evaluator. A discovery run should retain its policy version with its result evidence.

## Registry / Watchlist lifecycle

The target lifecycle is:

```text
DISCOVERED
-> QUALIFIED
-> QUEUED
-> BACKFILLING
-> BRONZE_READY
-> ACTIVE

BACKFILLING
-> FAILED
-> RETRY
-> PAUSED

Optional terminal state: RETIRED
```

The registry is control metadata. Raw game/review records remain in HDFS Bronze. A future registry entry should track the AppID, canonical name, lifecycle state, discovery and qualification timestamps, policy version, backfill status, source cursor/version, polling state, and failure/retry reason.

Discovery Control Plane V1 uses `data/raw/registry/game_registry.jsonl` at runtime, separate from the immutable research snapshot. If the registry does not exist, `selected_50_games.jsonl` seeds 50 `ACTIVE` entries with source `INITIAL_RESEARCH_SNAPSHOT`. Reconciliation deduplicates by AppID, preserves every existing lifecycle state, creates unseen qualified games as `NEW`, preserves outstanding `QUEUED` entries without duplicates, fills only the remaining cycle and ACTIVE-capacity slots, and leaves excess games `NEW`/deferred.

`src/discovery/run_discovery.py` produces a deterministic onboarding plan, a crawler-compatible JSONL queue, and a reconciled run report. It does not start historical backfill directly; the scheduler delegates that phase to the existing locked, resumable onboarding workflow. A production registry service and multi-user administration remain **Design-only**.

## NEW versus ACTIVE flow

```text
NEW game
-> Historical Backfill
-> Ingestion Integrity Validation
-> HDFS Bronze
-> verification/reconciliation
-> ACTIVE

ACTIVE game
-> Steam API polling
-> detect changes
-> Event Producer
-> Kafka
-> Raw Event Archive / Structured Streaming
```

Only `NEW` games receive the normal historical backfill. An `ACTIVE` game is not fully recrawled each weekly discovery cycle; an explicit rebuild is a separate auditable operation.

Before Bronze, Ingestion Integrity Validation may check HTTP readability, JSON parsing, expected AppID, file integrity, crawl completeness, record counts, and existing checksums. Analytical outlier removal, normalization, aggregation, feature engineering, and ML filtering belong to Bronze-to-Silver or later.

## Retry policy

The configured default is three attempts with exponential backoff. The intended transient sequence is 2, 4, and 8 seconds for HTTP 429, HTTP 5xx, timeouts, and connection resets; a valid Steam `Retry-After` value takes precedence. One game failure should be recorded and isolated rather than terminating an entire future discovery/onboarding cycle. Existing crawler retry behavior should be reused and aligned with this policy instead of duplicated.

## Policy version and change semantics

The current policy is version 1. Future discovery evidence should record the policy version used. Configuration history does not require a policy database in this slice.

Policy changes are non-retroactive by default. If Game A became `ACTIVE` under version 1 and version 2 raises `min_total_reviews`, Game A remains `ACTIVE`. Catalog refresh does not demote it. The new policy applies to future discovery/onboarding. Re-evaluating existing `ACTIVE` games is a separate explicit **Design-only** operation.

## Current research snapshot

`data/raw/selected_50_games.jsonl` remains **Initial Research Registry Snapshot / Dataset Snapshot v1**:

- 50 games
- 500 reviews per game
- 25,000 reviews total
- reproducible input for EDA, ML, MapReduce validation, reporting, and performance baselines

It is not an architecture limit and is not replaced by a production registry in this slice.

## Future administration interface — Planned / Design-only

A future interface may expose `GET /api/admin/discovery-policy` and `PUT /api/admin/discovery-policy`, with fields for minimum game age, minimum total reviews, metadata/review-endpoint requirements, historical reviews per game, maximum new games per cycle, discovery frequency, and retries. No frontend or production admin API is implemented in this slice.
