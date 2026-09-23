# Spark Analytics Design

## Status and input

Spark SQL / EDA is **Planned** and begins only after Silver validation and a Gold base are available. EDA consumes curated datasets; it does not repeat raw ingestion cleaning.

## Planned analytical questions

1. **Recommendation rate by game and genre**
   - review count, positive count, recommendation rate
   - minimum-support thresholds so tiny groups are not overinterpreted
2. **Playtime versus recommendation**
   - compare recommendation rate across documented playtime buckets
   - inspect skew/outliers rather than assuming a linear relationship
3. **Free versus paid games**
   - compare volume, engagement, playtime, and recommendation rate
   - preserve price/snapshot context for interpretation
4. **Top games and engagement**
   - review volume, average playtime, votes, and weighted engagement indicators
   - distinguish popularity from recommendation quality
5. **Data and label profiling**
   - class balance, missing metadata, duplicate/key rates, and feature distributions

## Required Gold Fields

Analytics cần Gold Base giữ tối thiểu:

### Game metadata
- appid
- name
- genres
- categories
- is_free
- price
- platforms
- release_date

### Review / behavior
- recommendationid
- voted_up
- playtime_at_review
- playtime_forever
- steam_purchase
- received_for_free
- timestamp_created

### Engagement
- votes_up
- votes_funny
- weighted_vote_score

### Derived fields
- recommendation_label
- playtime_hours
- playtime_bucket
- price_bucket

Gold Base grain:

1 row = 1 Steam review

Primary review key:

recommendationid

Join:

reviews.appid = games.appid

Genre and other array-valued metadata require a documented explode/normalization rule so one review/game does not accidentally inflate counts.

## Expected Analytics Outputs

### Q1 — Recommendation rate by game and genre

Output:
- game / genre
- review_count
- positive_count
- recommendation_rate
- average_playtime

### Q2 — Playtime versus recommendation

Output:
- playtime_bucket
- review_count
- positive_count
- recommendation_rate

Playtime buckets:
- 0–2 hours
- 2–10 hours
- 10–50 hours
- 50+ hours

### Q3 — Free versus paid games

Output:
- is_free
- game_count
- review_count
- average_playtime
- recommendation_rate
 
### Q4 — Top games and engagement

Output:
- appid
- name
- review_count
- average_playtime
- recommendation_rate
- votes / engagement metrics

### Q5 — Data and label profiling

Output:
- voted_up class balance
- missing/null counts
- duplicate recommendationid count
- invalid/missing appid count
- feature distributions
- playtime skew/outliers


## Evidence Outputs

Q1:
- evidence/analytics/game_metrics.parquet
- evidence/analytics/genre_metrics.parquet

Q2:
- evidence/analytics/playtime_buckets.csv

Q3:
- evidence/analytics/free_paid_metrics.csv

Q4:
- evidence/analytics/top_games_engagement.csv

Q5:
- evidence/analytics/data_label_profile.txt

Spark physical plan:
- evidence/spark/query_plan.txt

## Spark SQL and physical plans

Important queries should retain `df.explain("formatted")` evidence. Reviews are expected to be much larger than the 50-row game dimension, so the game join is a candidate for broadcast after size verification. Otherwise Spark may choose a sort-merge join.

Operators to interpret include:

- `Exchange` for shuffle boundaries
- hash/sort aggregates for grouped metrics
- `BroadcastHashJoin` versus `SortMergeJoin`
- scans, filters, projections, and partition pruning

A shuffle is expected for high-cardinality `groupBy` operations unless upstream partitioning satisfies the requirement. Partition counts should be based on measured input size and executor capacity, not arbitrary tuning.

## Evidence and outputs

Planned evidence includes query text, snapshot/version, row counts, formatted plans, measured runtime, shuffle read/write, and representative results. Gold analytical tables should remain reproducible from the same validated Silver snapshot.
