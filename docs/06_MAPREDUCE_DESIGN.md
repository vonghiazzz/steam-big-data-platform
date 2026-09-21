# MapReduce Design

## Status and purpose

The MapReduce job is **Planned**. It provides a meaningful distributed aggregation and an independent correctness check against equivalent PySpark aggregation; it is not a replacement for Bronze-to-Silver data engineering.

## Input and mapper

The job consumes review records with valid `appid`, `voted_up`, and `playtime_at_review`. The preferred input is a stable validated contract produced from the raw review scope, or a parser that explicitly rejects malformed Bronze records.

Conceptual mapper output:

```text
appid -> (1, positive_flag, playtime_at_review)
```

Where:

- `1` contributes to review count
- `positive_flag` is `1` when `voted_up=true`, otherwise `0`
- `playtime_at_review` contributes to an additive playtime sum

Diagnostics and malformed-record counters must go to stderr/counters, while stdout contains only key/value records.

## Shuffle, grouping, and optional combiner

Hadoop partitions mapper output by `appid`, shuffles records across the cluster, sorts them, and groups all values for the same game before reduction. A combiner may safely sum the three additive values because count, positive count, and playtime sum are associative and commutative. Correctness must not depend on the combiner running.

The combiner must not calculate recommendation rate or average playtime from partial groups. It emits the same intermediate state so Hadoop may execute it zero, one, or multiple times.

## Reducer metrics

For each `appid`, the reducer totals:

```text
review_count       = SUM(count)
positive_count     = SUM(positive_flag)
recommendation_rate = positive_count / review_count
average_playtime    = SUM(playtime_at_review) / review_count
```

Output should use deterministic field order and documented numeric formatting. Zero-count groups are invalid and should never be emitted.

## Spark validation

PySpark will calculate the same metrics using `groupBy("appid")` and equivalent null/type rules. Validation should compare:

- number and set of `appid` groups
- per-game review and positive counts
- recommendation rate within the chosen exact/rounding contract
- average playtime within the chosen exact/rounding contract
- aggregate input/output reconciliation

The validation must use the same versioned input snapshot. A mismatch should be diagnosed as parsing, filtering, grouping, null handling, or numeric-format behavior rather than hidden by changing expected results.
