# Machine Learning Design

## Status and Objective

Spark MLlib V1 is implemented and runtime validated.

The objective is binary classification:

```text
voted_up
```

Labels:

```text
1 = recommended

0 = not recommended
```

The model predicts recommendation behavior from:

- Player behavior features.
- Game metadata features.

Review text is not used in the primary model.

---

# ML Data Source

MLlib uses the operational Current Gold snapshot:

```text
/steam/gold/base_current_v1
```

Current Gold combines:

```text
Historical Gold

        +

Incremental Gold

        |

        v

Current Gold
```

The dataset is validated before training.

Required properties:

- One row per `recommendationid`.
- Stable schema.
- Valid binary target.
- Reproducible snapshot.

---

# ML Workflow

The training pipeline:

```text
Current Gold

      |

Dataset Validation

      |

Feature Preparation

      |

Train/Test Split

      |

Model Training

      |

Evaluation

      |

Versioned Output
```

Each run records:

- UTC run ID.
- Dataset fingerprint.
- Input path.
- Feature contract.
- Split information.
- Metrics.
- Model output paths.

---

# Feature Design

## Player Behavior Features

Examples:

- `playtime_at_review`
- `steam_purchase`
- `received_for_free`

---

## Game Metadata Features

Examples:

- `is_free`
- `price`
- `genres`
- `categories`
- `platforms`

---

## Feature Processing

Categorical features:

```text
genres
categories
```

are transformed using vectorization.

Platform fields are converted into explicit boolean features.

Nullable values are handled during the training pipeline.

---

# Leakage Prevention

Features must represent information available at prediction time.

Excluded examples:

- Post-review accumulated metrics.
- Target-derived recommendation rates.
- Aggregates containing the prediction row.
- Future information.

The feature contract is recorded for every ML run.

---

# Models

MLlib V1 compares:

## Majority Baseline

Purpose:

```text
Compare against class imbalance baseline
```

---

## Logistic Regression

Purpose:

```text
Interpretable linear model
```

---

## Random Forest

Purpose:

```text
Nonlinear model and feature importance analysis
```

---

# Evaluation Metrics

Each run records:

- Accuracy.
- Precision.
- Recall.
- F1-score.
- ROC-AUC.
- PR-AUC.
- Confusion Matrix.

Metrics must be interpreted together with class balance.

A higher accuracy alone does not guarantee better classification when the
dataset is imbalanced.

---

# Retraining Automation

ML retraining is controlled by Current Refresh Scheduler.

Flow:

```text
Current Gold Refresh

        |

Current Analytics Refresh

        |

Retrain Policy

        |

        +----------------+
        |                |

        v                v

     Skip ML          Retrain ML
```

Current policy:

```text
Minimum new rows:
1000

Maximum model age:
7 days
```

The scheduler avoids unnecessary retraining when the dataset has not changed
enough.

---

# Model Versioning

Every successful training run receives:

```text
run_id
```

Example:

```text
20261001T165508Z
```

Models:

```text
/steam/models/mllib/v1/<run_id>/
```

Contains:

```text
logistic_regression

random_forest
```

---

# Prediction Outputs

Prediction results:

```text
/steam/ml/mllib/v1/test_predictions/<run_id>/
```

Each prediction stores:

- recommendationid
- label
- prediction
- probability
- model name

---

# Evidence and Reproducibility

Evidence:

```text
evidence/ml/runs/<run_id>/
```

Contains:

- dataset profile
- split profile
- feature contract
- metrics
- confusion matrix
- feature importance
- training summary

The evidence allows the exact training run to be reproduced and reviewed.

---

# Running ML Manually

Preferred operational flow:

```bash
bash scripts/run_current_refresh.sh --run-once
```

For direct execution:

```bash
export ML_INPUT_PATH="/steam/gold/base_current_v1"

spark-submit \
  --master 'local[2]' \
  src/ml/run_mllib_v1.py
```

---

# Current Limitations

- MLlib V1 is batch retraining only.
- No online learning.
- No model serving API.
- No automatic model promotion.
- No A/B testing.

---

# Future Extensions

Possible future improvements:

- Model registry.
- Automatic model comparison.
- Prediction serving API.
- Feature store.
- Online inference pipeline.