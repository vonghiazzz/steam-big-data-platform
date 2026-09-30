"""Reusable Matplotlib charts for the small Gold Analytics outputs."""

from pathlib import Path
from typing import Iterable

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import PercentFormatter


PLAYTIME_BUCKET_ORDER = ["0-2h", "2-10h", "10-50h", "50h+", "Missing"]
FREE_PAID_ORDER = ["FREE", "PAID"]
PURCHASE_SOURCE_ORDER = ["STEAM_PURCHASE", "OTHER_SOURCE"]


def _require_columns(frame: pd.DataFrame, columns: Iterable[str]) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"Missing chart columns: {', '.join(missing)}")
    if frame.empty:
        raise ValueError("Cannot plot an empty DataFrame")


def _output_path(output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _save_and_close(fig, output_path: str | Path) -> Path:
    path = _output_path(output_path)
    try:
        fig.tight_layout()
        fig.savefig(path, dpi=160, bbox_inches="tight")
    finally:
        plt.close(fig)
    return path


def _format_percentage_axis(axis, orientation: str) -> None:
    formatter = PercentFormatter(xmax=1.0, decimals=0)
    if orientation == "x":
        axis.xaxis.set_major_formatter(formatter)
        axis.set_xlim(0, 1)
    else:
        axis.yaxis.set_major_formatter(formatter)
        axis.set_ylim(0, 1)


def select_top_games(frame: pd.DataFrame, limit: int = 10) -> pd.DataFrame:
    """Return a deterministic top-N while retaining review counts."""
    _require_columns(
        frame,
        ("game_name", "review_count", "recommendation_rate"),
    )
    return (
        frame.sort_values(
            ["recommendation_rate", "review_count", "game_name"],
            ascending=[False, False, True],
            kind="stable",
        )
        .head(limit)
        .reset_index(drop=True)
    )


def order_playtime_buckets(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize missing buckets and apply the analytical bucket order."""
    _require_columns(
        frame,
        ("playtime_bucket", "review_count", "recommendation_rate"),
    )
    ordered = frame.copy()
    ordered["playtime_bucket"] = (
        ordered["playtime_bucket"]
        .fillna("Missing")
        .replace(r"^\s*$", "Missing", regex=True)
    )
    rank = {bucket: index for index, bucket in enumerate(PLAYTIME_BUCKET_ORDER)}
    ordered["_bucket_rank"] = ordered["playtime_bucket"].map(rank).fillna(
        len(rank)
    )
    return (
        ordered.sort_values(
            ["_bucket_rank", "playtime_bucket"],
            ascending=[True, True],
            kind="stable",
        )
        .drop(columns="_bucket_rank")
        .reset_index(drop=True)
    )


def prepare_label_distribution(frame: pd.DataFrame) -> pd.DataFrame:
    """Convert the single profile row into the two plotted label groups."""
    _require_columns(
        frame,
        (
            "positive_count",
            "negative_count",
            "positive_percentage",
            "negative_percentage",
        ),
    )
    if len(frame.index) != 1:
        raise ValueError("Label profile must contain exactly one row")
    profile = frame.iloc[0]
    return pd.DataFrame(
        {
            "label": ["Positive", "Negative"],
            "count": [profile["positive_count"], profile["negative_count"]],
            "percentage": [
                profile["positive_percentage"],
                profile["negative_percentage"],
            ],
        }
    )


def plot_top_games(frame: pd.DataFrame, output_path: str | Path) -> Path:
    selected = select_top_games(frame)
    display = selected.iloc[::-1]
    fig, axis = plt.subplots(figsize=(10, 6))
    axis.barh(display["game_name"], display["recommendation_rate"])
    axis.set_title("Top 10 Games by Recommendation Rate")
    axis.set_xlabel("Recommendation rate")
    axis.set_ylabel("Game")
    _format_percentage_axis(axis, "x")
    return _save_and_close(fig, output_path)


def _select_genres(frame: pd.DataFrame, max_genres: int) -> tuple[pd.DataFrame, str]:
    _require_columns(frame, ("genre", "review_count", "recommendation_rate"))
    if len(frame.index) <= max_genres:
        return frame.copy(), "Recommendation Rate by Genre"

    selected = frame.sort_values(
        ["review_count", "genre"],
        ascending=[False, True],
        kind="stable",
    ).head(max_genres)
    unknown = frame[frame["genre"] == "UNKNOWN"]
    if not unknown.empty and "UNKNOWN" not in set(selected["genre"]):
        selected = pd.concat([selected.iloc[:-1], unknown.head(1)], ignore_index=True)
    return selected, (
        f"Recommendation Rate by Genre "
        f"(Top {max_genres} by Review Count; UNKNOWN Retained)"
    )


def plot_genre_recommendation(
    frame: pd.DataFrame,
    output_path: str | Path,
    max_genres: int = 25,
) -> Path:
    selected, title = _select_genres(frame, max_genres)
    display = selected.sort_values(
        ["recommendation_rate", "review_count", "genre"],
        ascending=[True, True, True],
        kind="stable",
    )
    height = max(6.0, min(14.0, 0.38 * len(display.index) + 2.0))
    fig, axis = plt.subplots(figsize=(10, height))
    axis.barh(display["genre"], display["recommendation_rate"])
    axis.set_title(title)
    axis.set_xlabel("Recommendation rate")
    axis.set_ylabel("Genre (multi-membership)")
    _format_percentage_axis(axis, "x")
    return _save_and_close(fig, output_path)


def plot_playtime_recommendation(
    frame: pd.DataFrame,
    output_path: str | Path,
) -> Path:
    ordered = order_playtime_buckets(frame)
    fig, axis = plt.subplots(figsize=(9, 5))
    axis.bar(ordered["playtime_bucket"], ordered["recommendation_rate"])
    axis.set_title("Recommendation Rate by Playtime Bucket")
    axis.set_xlabel("Playtime bucket")
    axis.set_ylabel("Recommendation rate")
    _format_percentage_axis(axis, "y")
    return _save_and_close(fig, output_path)


def plot_free_paid_recommendation(
    frame: pd.DataFrame,
    output_path: str | Path,
) -> Path:
    _require_columns(
        frame,
        ("game_type", "game_count", "review_count", "recommendation_rate"),
    )
    rank = {group: index for index, group in enumerate(FREE_PAID_ORDER)}
    ordered = frame.assign(_rank=frame["game_type"].map(rank)).sort_values(
        ["_rank", "game_type"],
        kind="stable",
    )
    fig, axis = plt.subplots(figsize=(7, 5))
    axis.bar(ordered["game_type"], ordered["recommendation_rate"])
    axis.set_title("Recommendation Rate: Free vs Paid Games")
    axis.set_xlabel("Game type")
    axis.set_ylabel("Recommendation rate")
    _format_percentage_axis(axis, "y")
    return _save_and_close(fig, output_path)


def plot_label_distribution(
    frame: pd.DataFrame,
    output_path: str | Path,
) -> Path:
    distribution = prepare_label_distribution(frame)
    fig, axis = plt.subplots(figsize=(7, 5))
    bars = axis.bar(distribution["label"], distribution["count"])
    axis.set_title("Positive vs Negative Review Distribution")
    axis.set_xlabel("Review label")
    axis.set_ylabel("Review count")
    for bar, (_, row) in zip(bars, distribution.iterrows()):
        axis.annotate(
            f"{int(row['count']):,} ({row['percentage']:.1%})",
            xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
            xytext=(0, 4),
            textcoords="offset points",
            ha="center",
            va="bottom",
        )
    return _save_and_close(fig, output_path)


def plot_purchase_recommendation(
    frame: pd.DataFrame,
    output_path: str | Path,
) -> Path:
    _require_columns(
        frame,
        ("purchase_source", "review_count", "recommendation_rate"),
    )
    rank = {
        group: index for index, group in enumerate(PURCHASE_SOURCE_ORDER)
    }
    ordered = frame.assign(
        _rank=frame["purchase_source"].map(rank)
    ).sort_values(["_rank", "purchase_source"], kind="stable")
    display_labels = ordered["purchase_source"].replace(
        {
            "STEAM_PURCHASE": "Steam Purchase",
            "OTHER_SOURCE": "Other Source",
        }
    )
    fig, axis = plt.subplots(figsize=(8, 5))
    axis.bar(display_labels, ordered["recommendation_rate"])
    axis.set_title("Recommendation Rate by Purchase Source")
    axis.set_xlabel("Purchase source")
    axis.set_ylabel("Recommendation rate")
    _format_percentage_axis(axis, "y")
    return _save_and_close(fig, output_path)
