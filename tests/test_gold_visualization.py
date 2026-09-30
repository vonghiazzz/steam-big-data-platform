import tempfile
import unittest
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from src.visualization.gold_charts import (
    order_playtime_buckets,
    plot_label_distribution,
    prepare_label_distribution,
    select_top_games,
)


class GoldVisualizationV1Test(unittest.TestCase):
    def test_top_games_is_deterministic_and_selects_exactly_ten(self):
        frame = pd.DataFrame(
            {
                "game_name": [f"Game {index:02d}" for index in range(12)],
                "review_count": [500] * 12,
                "recommendation_rate": [
                    0.50,
                    0.60,
                    0.70,
                    0.80,
                    0.90,
                    0.91,
                    0.92,
                    0.93,
                    0.94,
                    0.95,
                    0.96,
                    0.97,
                ],
            }
        )
        selected = select_top_games(frame)
        self.assertEqual(len(selected.index), 10)
        self.assertEqual(selected.iloc[0]["game_name"], "Game 11")
        self.assertEqual(selected.iloc[-1]["game_name"], "Game 02")
        self.assertNotIn("Game 00", set(selected["game_name"]))
        self.assertNotIn("Game 01", set(selected["game_name"]))
        self.assertTrue((selected["review_count"] == 500).all())

    def test_playtime_order_and_missing_label(self):
        frame = pd.DataFrame(
            {
                "playtime_bucket": ["50h+", None, "2-10h", "0-2h", "10-50h"],
                "review_count": [1, 1, 1, 1, 1],
                "recommendation_rate": [0.9, 0.5, 0.7, 0.6, 0.8],
            }
        )
        ordered = order_playtime_buckets(frame)
        self.assertEqual(
            list(ordered["playtime_bucket"]),
            ["0-2h", "2-10h", "10-50h", "50h+", "Missing"],
        )

    def test_label_values_and_png_creation_close_figures(self):
        profile = pd.DataFrame(
            [
                {
                    "positive_count": 18_321,
                    "negative_count": 6_679,
                    "positive_percentage": 18_321 / 25_000,
                    "negative_percentage": 6_679 / 25_000,
                }
            ]
        )
        distribution = prepare_label_distribution(profile)
        self.assertEqual(list(distribution["label"]), ["Positive", "Negative"])
        self.assertEqual(list(distribution["count"]), [18_321, 6_679])

        plt.close("all")
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "nested" / "label_distribution.png"
            created = plot_label_distribution(profile, output)
            self.assertEqual(created, output)
            self.assertTrue(output.is_file())
            self.assertGreater(output.stat().st_size, 0)
        self.assertEqual(plt.get_fignums(), [])


if __name__ == "__main__":
    unittest.main()
