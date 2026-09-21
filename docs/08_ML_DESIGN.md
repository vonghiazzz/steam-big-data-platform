# Machine Learning Design

## Status and objective

Spark MLlib modeling is **Planned**. The binary target is `voted_up` (`1` recommend, `0` not recommend). The experiment asks whether player behavior and game characteristics can predict recommendation behavior.

The primary model does not use review text, so NLP is outside the primary experiment.

## Features

### Player behavior

- `playtime_at_review`
- `playtime_forever`
- `steam_purchase`
- `received_for_free`

### Game metadata

- `is_free`
- price derived from the relevant price snapshot
- `genres`
- `platforms`
- `categories`

Arrays/categorical fields require deterministic encoding. Numeric playtime may need log transformation or robust handling because Steam playtime is strongly skewed.

## Models and evaluation

Two planned models provide complementary baselines:

- **Logistic Regression:** interpretable linear baseline
- **Random Forest:** nonlinear interactions and feature importance

Report at least:

- Accuracy
- Precision
- Recall
- F1
- ROC-AUC
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

## Reproducible snapshots

ML experiments use fixed, versioned Silver/Gold snapshots. The current 50-game, 25,000-review cohort is one such research snapshot, not a registry limit. Each run should record snapshot/version, schema, feature list, split seed/rule, preprocessing parameters, model hyperparameters, software version, and evaluation outputs. Dynamic onboarding or streaming updates must not silently mutate the dataset behind a reported experiment.
