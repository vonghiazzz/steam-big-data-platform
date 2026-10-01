# Machine Learning Design

## Status and objective

Spark MLlib V1 is **implemented and runtime validated**. The binary target is
`voted_up` (`1` recommend, `0` not recommend). Each run is a fresh batch
retraining over the current dynamic `/steam/gold/base`; it is not online model
learning.

The primary model does not use review text, so NLP is outside the primary experiment.

## Features

### Player behavior

- `log1p(playtime_at_review)`
- `steam_purchase`
- `received_for_free`

### Game metadata

- `is_free`
- `log1p(price)` derived from crawl-time game metadata
- `genres`
- `platforms`
- `categories`

Genres and categories use binary `CountVectorizer`; platforms use explicit
Windows/macOS/Linux flags. Imputation and vocabularies are fitted on training
data only. `playtime_forever` is excluded from the primary model because it may
contain information observed after review creation.

## Models and evaluation

The majority-class baseline and two trained models provide complementary
comparisons:

- **Logistic Regression:** interpretable linear baseline
- **Random Forest:** nonlinear interactions and feature importance

Report at least:

- Accuracy
- Precision
- Recall
- F1
- ROC-AUC
- PR-AUC
- Confusion Matrix

Metrics must be interpreted with class balance. Model comparison should use the same train/test split, seed, feature contract, and dataset snapshot.

## Leakage controls

Features must be available at the intended prediction time. Potential leakage includes:

- `votes_up` or `weighted_vote_score` accumulated after the review/target exists
- updated playtime measured long after `playtime_at_review`
- aggregates that include the row being predicted
- target-derived game recommendation rates
- random row splits that let the same user/game context leak across partitions of an evaluation design

The experiment should document the prediction scenario before accepting a feature. Where appropriate, use temporal or grouped splits and compare them with a simple seeded baseline split.

## Dynamic snapshot reproducibility

Gold Base grows through onboarding, so counts are integrity observations rather
than fixed acceptance constants. Each run records a UTC run ID, input path,
game/review/label counts, deterministic dataset fingerprint, split seed,
feature contract, class weights, model parameters, metrics, and output paths.
Evidence is stored under `evidence/ml/`; PipelineModels are saved under
`/steam/models/mllib/v1`.

## Model persistence

Each V1 run currently overwrites
`/steam/models/mllib/v1/logistic_regression` and
`/steam/models/mllib/v1/random_forest`. Model versioning, retention, comparison,
and promotion are future work and are intentionally outside MLlib V1.
