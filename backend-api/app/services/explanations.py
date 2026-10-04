from __future__ import annotations

from math import factorial
from numbers import Real
from typing import Mapping, Sequence


FEATURE_ATTRIBUTES = (
    ("playtime_at_review", "Playtime at review"),
    ("steam_purchase", "Purchased on Steam"),
    ("received_for_free", "Received for free"),
    ("is_free", "Free-to-play"),
    ("price", "Game price"),
    ("genres", "Genres"),
    ("categories", "Categories"),
    ("platforms", "Platforms"),
)


def exact_grouped_shap_values(
    probabilities: Sequence[float],
) -> list[dict[str, float | str]]:
    """Compute exact interventional Shapley values from all feature coalitions."""
    feature_count = len(FEATURE_ATTRIBUTES)
    expected_count = 1 << feature_count
    if len(probabilities) != expected_count:
        raise ValueError(
            f"Expected {expected_count} coalition probabilities, got "
            f"{len(probabilities)}"
        )
    if any(
        isinstance(value, bool)
        or not isinstance(value, Real)
        or not 0.0 <= float(value) <= 1.0
        for value in probabilities
    ):
        raise ValueError("Coalition probabilities must be numeric values in [0, 1]")

    denominator = factorial(feature_count)
    result = []
    for feature_index, (attribute, label) in enumerate(FEATURE_ATTRIBUTES):
        feature_bit = 1 << feature_index
        contribution = 0.0
        for coalition in range(expected_count):
            if coalition & feature_bit:
                continue
            coalition_size = coalition.bit_count()
            weight = (
                factorial(coalition_size)
                * factorial(feature_count - coalition_size - 1)
                / denominator
            )
            contribution += weight * (
                float(probabilities[coalition | feature_bit])
                - float(probabilities[coalition])
            )
        result.append({"attribute": attribute, "label": label, "value": contribution})

    result.sort(key=lambda item: -abs(float(item["value"])))
    return result


def grouped_global_importance(
    feature_names: Sequence[str],
    importances: Sequence[float],
) -> list[dict[str, float | str]]:
    """Aggregate encoded model importance by the input attribute users see."""
    if len(feature_names) != len(importances):
        raise ValueError("Feature names and importances must have the same length")

    totals = {attribute: 0.0 for attribute, _ in FEATURE_ATTRIBUTES}
    labels = dict(FEATURE_ATTRIBUTES)
    for name, importance in zip(feature_names, importances):
        if isinstance(importance, bool) or not isinstance(importance, Real):
            raise ValueError("Feature importances must be numeric")
        value = float(importance)
        if value < 0.0:
            raise ValueError("Feature importances cannot be negative")
        if name.startswith(("log1p_playtime_at_review",)):
            attribute = "playtime_at_review"
        elif name.startswith("steam_purchase"):
            attribute = "steam_purchase"
        elif name.startswith("received_for_free"):
            attribute = "received_for_free"
        elif name.startswith("is_free"):
            attribute = "is_free"
        elif name.startswith("log1p_price"):
            attribute = "price"
        elif name.startswith("platform_"):
            attribute = "platforms"
        elif name.startswith("genre="):
            attribute = "genres"
        elif name.startswith("category="):
            attribute = "categories"
        else:
            raise ValueError(f"Unrecognized model feature name: {name}")
        totals[attribute] += value

    total_importance = sum(totals.values())
    if total_importance <= 0.0:
        raise ValueError("The model has no positive global feature importance")
    result = [
        {
            "attribute": attribute,
            "label": labels[attribute],
            "value": value / total_importance,
        }
        for attribute, value in totals.items()
        if value > 0.0
    ]
    result.sort(key=lambda item: -float(item["value"]))
    return result


def coalition_probabilities(
    rows: Sequence[Mapping[str, object]],
) -> list[float]:
    """Read exact coalition outputs from a result indexed by padded SHAP IDs."""
    expected_count = 1 << len(FEATURE_ATTRIBUTES)
    probabilities: list[float | None] = [None] * expected_count
    for row in rows:
        identity = str(row["recommendationid"])
        prefix, _, encoded_mask = identity.partition("-")
        if prefix != "shap" or not encoded_mask.isdigit():
            raise ValueError(f"Invalid SHAP coalition identity: {identity}")
        mask = int(encoded_mask)
        if mask >= expected_count or probabilities[mask] is not None:
            raise ValueError(f"Invalid or duplicate SHAP coalition: {identity}")
        value = row["probability_positive"]
        if isinstance(value, bool) or not isinstance(value, Real):
            raise ValueError(f"Invalid SHAP probability for coalition: {identity}")
        probabilities[mask] = float(value)
    if any(value is None for value in probabilities):
        raise ValueError("Spark scoring did not return every SHAP coalition")
    return [float(value) for value in probabilities]


def build_what_if_rows(
    request_payload: Mapping[str, object],
) -> tuple[list[dict[str, object]], dict[str, dict[str, object]]]:
    """Create one-at-a-time category and genre toggles around an input row."""
    options = request_payload.get("what_if_options", {})
    if not isinstance(options, Mapping):
        raise ValueError("what_if_options must be an object")

    rows: list[dict[str, object]] = []
    details: dict[str, dict[str, object]] = {}
    for feature_type, column in (("genre", "genres"), ("category", "categories")):
        values = request_payload[column]
        if not isinstance(values, list):
            raise ValueError(f"{column} must be a list")
        selected_values = set(values)
        candidates = options.get(column, [])
        if not isinstance(candidates, list):
            raise ValueError(f"what_if_options.{column} must be a list")
        for index, token in enumerate(dict.fromkeys(candidates)):
            if not isinstance(token, str):
                raise ValueError(f"what_if_options.{column} entries must be strings")
            selected = token in selected_values
            alternative_values = (
                [value for value in values if value != token]
                if selected
                else [*values, token]
            )
            identity = f"whatif-{feature_type}-{index:03d}"
            row = {
                key: request_payload[key]
                for key, _ in FEATURE_ATTRIBUTES
            }
            row["recommendationid"] = identity
            row[column] = alternative_values
            rows.append(row)
            details[identity] = {
                "feature_type": feature_type,
                "value": token,
                "selected": selected,
            }
    return rows, details


def build_what_if_effects(
    rows: Sequence[Mapping[str, object]],
    details: Mapping[str, Mapping[str, object]],
    current_probability: float,
) -> list[dict[str, object]]:
    """Calculate one-toggle probability changes from model-scored rows."""
    effects = []
    seen: set[str] = set()
    for row in rows:
        identity = str(row["recommendationid"])
        detail = details.get(identity)
        if detail is None:
            raise ValueError(f"Missing what-if metadata for row: {identity}")
        if identity in seen:
            raise ValueError(f"Duplicate what-if result: {identity}")
        seen.add(identity)
        probability = row["probability_positive"]
        if isinstance(probability, bool) or not isinstance(probability, Real):
            raise ValueError(f"Invalid what-if probability for row: {identity}")
        probability = float(probability)
        if not 0.0 <= probability <= 1.0:
            raise ValueError(f"Invalid what-if probability for row: {identity}")
        effects.append(
            {
                **detail,
                "probability_after_toggle": probability,
                "probability_delta": probability - current_probability,
            }
        )

    if len(seen) != len(details):
        raise ValueError("Spark scoring did not return every what-if option")
    effects.sort(
        key=lambda item: (
            -float(item["probability_delta"]),
            str(item["feature_type"]),
            str(item["value"]),
        )
    )
    return effects
