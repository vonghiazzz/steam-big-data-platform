"""Binary-classification metrics with explicit positive-class semantics."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class BinaryMetrics:
    model: str
    accuracy: float
    precision: float
    recall: float
    f1: float
    roc_auc: float | None
    pr_auc: float | None
    tp: int
    fp: int
    tn: int
    fn: int

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def evaluate_binary_predictions(
    predictions: DataFrame,
    *,
    model_name: str,
    score_column: str | None = "probability",
) -> BinaryMetrics:
    counts = predictions.agg(
        F.sum(
            F.when((F.col("label") == 1.0) & (F.col("prediction") == 1.0), 1).otherwise(0)
        ).alias("tp"),
        F.sum(
            F.when((F.col("label") == 0.0) & (F.col("prediction") == 1.0), 1).otherwise(0)
        ).alias("fp"),
        F.sum(
            F.when((F.col("label") == 0.0) & (F.col("prediction") == 0.0), 1).otherwise(0)
        ).alias("tn"),
        F.sum(
            F.when((F.col("label") == 1.0) & (F.col("prediction") == 0.0), 1).otherwise(0)
        ).alias("fn"),
    ).first()
    tp, fp, tn, fn = (int(counts[name] or 0) for name in ("tp", "fp", "tn", "fn"))
    total = tp + fp + tn + fn
    accuracy = (tp + tn) / total if total else 0.0
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2.0 * precision * recall / (precision + recall) if precision + recall else 0.0
    roc_auc = None
    pr_auc = None
    if score_column is not None:
        evaluator = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol=score_column,
        )
        roc_auc = float(evaluator.setMetricName("areaUnderROC").evaluate(predictions))
        pr_auc = float(evaluator.setMetricName("areaUnderPR").evaluate(predictions))
    return BinaryMetrics(
        model=model_name,
        accuracy=accuracy,
        precision=precision,
        recall=recall,
        f1=f1,
        roc_auc=roc_auc,
        pr_auc=pr_auc,
        tp=tp,
        fp=fp,
        tn=tn,
        fn=fn,
    )


def majority_baseline_predictions(
    train: DataFrame,
    test: DataFrame,
) -> tuple[float, DataFrame]:
    majority = (
        train.groupBy("label")
        .count()
        .orderBy(F.col("count").desc(), F.col("label").desc())
        .first()
    )
    if majority is None:
        raise RuntimeError("training split is empty")
    majority_label = float(majority["label"])
    return majority_label, test.withColumn("prediction", F.lit(majority_label))
