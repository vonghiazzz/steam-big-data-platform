import io
import json
import tempfile
import unittest
from pathlib import Path

from src.mapreduce.compare_game_metrics import (
    GameMetric,
    build_report,
    compare_metrics,
    read_metrics_tsv,
    write_metrics_tsv,
)
from src.mapreduce.game_recommendation_mapper import (
    map_stream,
    parse_review_line,
)
from src.mapreduce.game_recommendation_reducer import reduce_stream


def review(appid, voted_up):
    return json.dumps(
        {
            "appid": appid,
            "review": {"recommendationid": f"{appid}-{voted_up}", "voted_up": voted_up},
        }
    )


class GameRecommendationMapReduceTest(unittest.TestCase):
    def test_mapper_emits_positive_negative_and_multiple_appids(self):
        source = io.StringIO(
            "\n".join(
                [
                    review(10, True),
                    review(10, False),
                    review(20, True),
                    "",
                ]
            )
        )
        output = io.StringIO()
        errors = io.StringIO()
        self.assertEqual(map_stream(source, output, errors), 0)
        self.assertEqual(
            output.getvalue().splitlines(),
            ["10\t1\t1\t0", "10\t1\t0\t1", "20\t1\t1\t0"],
        )

    def test_mapper_rejects_malformed_json_and_non_boolean_label(self):
        source = io.StringIO(
            "{malformed\n"
            + json.dumps({"appid": 10, "review": {"voted_up": "true"}})
            + "\n"
        )
        output = io.StringIO()
        errors = io.StringIO()
        self.assertEqual(map_stream(source, output, errors), 0)
        self.assertEqual(output.getvalue(), "")
        self.assertEqual(errors.getvalue().count("MalformedRecords,1"), 2)
        self.assertIn("malformed JSON", errors.getvalue())
        self.assertIn("voted_up is not boolean", errors.getvalue())

    def test_mapper_falls_back_to_appid_in_input_filename(self):
        line = json.dumps({"review": {"voted_up": True}})
        self.assertEqual(
            parse_review_line(line, "/steam/bronze/reviews/730.jsonl"),
            ("730", 1, 1, 0),
        )

    def test_reducer_counts_and_rate_are_deterministic(self):
        source_text = (
            "10\t1\t1\t0\n"
            "10\t1\t0\t1\n"
            "10\t1\t1\t0\n"
            "20\t1\t0\t1\n"
        )
        expected = (
            "10\t3\t2\t1\t0.666666666667\n"
            "20\t1\t0\t1\t0.000000000000\n"
        )
        for _ in range(2):
            output = io.StringIO()
            errors = io.StringIO()
            self.assertEqual(
                reduce_stream(io.StringIO(source_text), output, errors),
                0,
            )
            self.assertEqual(output.getvalue(), expected)
            self.assertEqual(errors.getvalue(), "")

    def test_reducer_rejects_non_reconciling_intermediate_state(self):
        output = io.StringIO()
        errors = io.StringIO()
        self.assertEqual(
            reduce_stream(io.StringIO("10\t1\t1\t1\n"), output, errors),
            0,
        )
        self.assertEqual(output.getvalue(), "")
        self.assertIn("MalformedIntermediateRecords,1", errors.getvalue())
        self.assertIn("label counts do not reconcile", errors.getvalue())

    def test_comparator_matches_by_appid_not_row_position(self):
        first = GameMetric(10, 3, 2, 1, 2 / 3)
        second = GameMetric(20, 1, 0, 1, 0.0)
        mapreduce = {20: second, 10: first}
        spark = {10: first, 20: second}
        comparison = compare_metrics(mapreduce, spark)
        self.assertEqual(comparison.missing_in_mapreduce, ())
        self.assertEqual(comparison.unexpected_in_mapreduce, ())
        self.assertEqual(comparison.count_mismatches, ())
        self.assertEqual(comparison.rate_mismatches, ())

    def test_tsv_round_trip_is_sorted_and_machine_readable(self):
        metrics = [
            GameMetric(20, 1, 0, 1, 0.0),
            GameMetric(10, 3, 2, 1, 2 / 3),
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metrics.tsv"
            write_metrics_tsv(path, metrics)
            self.assertEqual(
                [line.split("\t", 1)[0] for line in path.read_text().splitlines()],
                ["10", "20"],
            )
            loaded = read_metrics_tsv(path)
        self.assertEqual(set(loaded), {10, 20})
        comparison = compare_metrics(loaded, {10: metrics[1], 20: metrics[0]})
        report, passed = build_report(loaded, loaded, comparison)
        self.assertFalse(passed)
        self.assertIn("CROSS-CHECK: FAIL", report)


if __name__ == "__main__":
    unittest.main()
