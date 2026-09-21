# Team Workflow

## Ownership

| Member | Primary ownership | Feature branch |
|---|---|---|
| Bình | HDFS, MapReduce, raw validation | `feature/binh-hdfs-mapreduce` |
| Khánh | PySpark data engineering, streaming | `feature/khanh-pyspark-streaming` |
| Nghĩa | EDA, Spark SQL, visualization, NoSQL | `feature/nghia-analytics-nosql` |
| Huy | Machine learning, performance, deployment | `feature/huy-ml-performance` |

Ownership identifies the primary driver, not a barrier to review or cross-checking. Shared data contracts—especially Bronze/Silver/Gold schemas, identifiers, snapshot versions, and output locations—require team agreement.

## Delivery flow

```text
feature branch
-> coherent feature slice
-> focused verification
-> evidence
-> docs update
-> Pull Request
-> Review
-> main
```

A coherent slice should have one clear outcome. For example, Bronze-to-Silver includes schema code, deterministic validation, reconciliation evidence, and documentation together; it should not mix unrelated ML or dashboard changes.

## Pull request expectations

Each PR should state:

- scope and owner
- implemented versus planned behavior
- input/output contracts and paths
- exact verification commands and observed results
- evidence locations
- data/schema compatibility impact
- risks, rollback/rebuild approach, and follow-up tasks

Large raw data, generated Parquet, checkpoints, secrets, and local environment artifacts must not be committed. Small schemas, queries, metrics, and focused evidence may be committed according to repository policy.

## Integration checkpoints

Cross-owner reviews are especially important at:

1. Bronze-to-Silver schema/reconciliation handoff
2. Silver-to-Gold analytics/ML feature contract
3. MapReduce-versus-Spark metric validation
4. Kafka event envelope and Structured Streaming Silver contract
5. Gold-to-MongoDB keys and read-back validation
6. model snapshot/features versus serving model version

Documentation must label components **Implemented**, **In Progress**, **Planned**, or **Design-only**. A diagram or proposal is not implementation evidence.
