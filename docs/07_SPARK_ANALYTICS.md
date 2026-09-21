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

Genre and other array-valued metadata require a documented explode/normalization rule so one review/game does not accidentally inflate counts.

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
