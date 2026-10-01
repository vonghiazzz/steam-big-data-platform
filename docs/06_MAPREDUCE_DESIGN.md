# MapReduce Design

## Purpose

The MapReduce component provides an independent aggregation path to validate
Spark analytical results.

It is a validation pipeline, not the main analytics engine.

Flow:

```text
HDFS Bronze Reviews

        |

        v

Hadoop Streaming MapReduce

        |

        v

Game-level Metrics

        |

        v

Compare with Spark Results
```

---

# Input Data

MapReduce consumes validated review records.

Required fields:

```text
appid

voted_up

playtime_at_review
```

Input data should come from a reproducible snapshot.

Malformed records should be counted separately instead of silently affecting
aggregation results.

---

# Mapper

Mapper converts each review into an intermediate key/value pair.

Concept:

```text
appid -> (count, positive_count, playtime_sum)
```

Example:

Input:

```text
appid=570

voted_up=true

playtime_at_review=120
```

Output:

```text
570 -> (1,1,120)
```

---

# Shuffle and Grouping

Hadoop automatically performs:

```text
Mapper Output

        |

        v

Partition

        |

        v

Shuffle

        |

        v

Sort

        |

        v

Reducer Grouping
```

All records with the same `appid` are processed together.

---

# Combiner

A combiner can optimize intermediate aggregation.

Safe operations:

```text
review count

+

positive count

+

playtime sum
```

The combiner must not calculate:

- recommendation rate.
- average playtime.

Those require final reducer results.

---

# Reducer

Reducer produces game-level metrics.

For each:

```text
appid
```

Calculate:

```text
review_count

positive_count

recommendation_rate

average_playtime
```

Formula:

```text
recommendation_rate =
positive_count / review_count


average_playtime =
playtime_sum / review_count
```

---

# Spark Validation

Spark independently calculates equivalent metrics.

Comparison checks:

- Same appid groups.
- Same review counts.
- Same positive counts.
- Same recommendation rate.
- Same average playtime.
- Same reconciliation totals.

The comparison must use the same input snapshot.

---

# Execution Role

MapReduce provides:

```text
Independent implementation

        +

Distributed processing example

        +

Spark correctness validation
```

It does not replace:

- Spark SQL analytics.
- Gold generation.
- MLlib training.

---

# Evidence

Validation evidence should include:

- Input snapshot.
- Mapper/reducer output.
- Spark comparison result.
- Mismatch count.

Expected result:

```text
CROSS-CHECK: PASS
```

with zero metric mismatches.

---

# Future Extensions

Possible improvements:

- Larger distributed datasets.
- Additional aggregation jobs.
- Automated comparison reports.
- Multi-snapshot validation.