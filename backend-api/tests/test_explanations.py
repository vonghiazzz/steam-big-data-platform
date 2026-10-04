import pytest

from app.services.explanations import (
    FEATURE_ATTRIBUTES,
    build_what_if_effects,
    build_what_if_rows,
    coalition_probabilities,
    exact_grouped_shap_values,
    grouped_global_importance,
)


def test_exact_grouped_shap_values_reconstruct_additive_prediction():
    probabilities = [
        0.1
        + sum(
            (index + 1) * 0.005
            for index in range(len(FEATURE_ATTRIBUTES))
            if mask & (1 << index)
        )
        for mask in range(1 << len(FEATURE_ATTRIBUTES))
    ]

    contributions = exact_grouped_shap_values(probabilities)

    by_attribute = {item["attribute"]: item["value"] for item in contributions}
    assert sum(by_attribute.values()) == pytest.approx(probabilities[-1] - probabilities[0])
    assert by_attribute["playtime_at_review"] == pytest.approx(0.005)
    assert by_attribute["platforms"] == pytest.approx(0.04)


def test_exact_grouped_shap_rejects_incomplete_probabilities():
    with pytest.raises(ValueError, match="Expected 256"):
        exact_grouped_shap_values([0.5])


def test_grouped_global_importance_aggregates_encoded_features():
    result = grouped_global_importance(
        [
            "log1p_playtime_at_review_imputed",
            "platform_windows_numeric_imputed",
            "platform_mac_numeric_imputed",
            "genre=Action",
            "category=Online PvP",
        ],
        [0.1, 0.2, 0.1, 0.25, 0.35],
    )

    assert {item["attribute"]: item["value"] for item in result} == pytest.approx(
        {
        "categories": 0.35,
        "genres": 0.25,
        "platforms": 0.3,
        "playtime_at_review": 0.1,
        }
    )


def test_coalition_probabilities_restore_mask_order():
    rows = [
        {"recommendationid": "shap-255", "probability_positive": 0.8},
        {"recommendationid": "shap-000", "probability_positive": 0.2},
        {"recommendationid": "shap-001", "probability_positive": 0.3},
    ]

    with pytest.raises(ValueError, match="every SHAP coalition"):
        coalition_probabilities(rows)


def test_what_if_rows_toggle_one_category_or_genre_at_a_time():
    payload = {
        "playtime_at_review": 600,
        "steam_purchase": True,
        "received_for_free": False,
        "is_free": False,
        "price": 19.99,
        "genres": ["Action"],
        "categories": ["Single-player"],
        "platforms": {"windows": True, "mac": False, "linux": None},
        "what_if_options": {
            "genres": ["Action", "RPG"],
            "categories": ["Single-player", "Online PvP"],
        },
    }

    rows, details = build_what_if_rows(payload)

    assert len(rows) == 4
    row_by_id = {row["recommendationid"]: row for row in rows}
    assert row_by_id["whatif-genre-000"]["genres"] == []
    assert row_by_id["whatif-genre-001"]["genres"] == ["Action", "RPG"]
    assert row_by_id["whatif-category-000"]["categories"] == []
    assert row_by_id["whatif-category-001"]["categories"] == [
        "Single-player",
        "Online PvP",
    ]
    assert details["whatif-category-000"] == {
        "feature_type": "category",
        "value": "Single-player",
        "selected": True,
    }
    assert all(
        row["playtime_at_review"] == 600
        and row["price"] == 19.99
        and row["platforms"] == payload["platforms"]
        for row in rows
    )


def test_what_if_effects_report_probability_delta_and_reject_missing_results():
    details = {
        "whatif-category-000": {
            "feature_type": "category",
            "value": "Online PvP",
            "selected": False,
        }
    }
    effects = build_what_if_effects(
        [
            {
                "recommendationid": "whatif-category-000",
                "probability_positive": 0.64,
            }
        ],
        details,
        current_probability=0.6,
    )

    assert effects == [
        {
            "feature_type": "category",
            "value": "Online PvP",
            "selected": False,
            "probability_after_toggle": 0.64,
            "probability_delta": pytest.approx(0.04),
        }
    ]
    with pytest.raises(ValueError, match="every what-if option"):
        build_what_if_effects([], details, current_probability=0.6)
