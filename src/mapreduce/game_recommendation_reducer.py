#!/usr/bin/env python3
"""Reduce sorted additive state to recommendation metrics by appid."""

import sys
from typing import TextIO


COUNTER_GROUP = "Steam MapReduce Cross-check"


def _counter(error_stream: TextIO, name: str) -> None:
    print(f"reporter:counter:{COUNTER_GROUP},{name},1", file=error_stream)


def parse_intermediate(raw_line: str) -> tuple[str, int, int, int]:
    fields = raw_line.rstrip("\r\n").split("\t")
    if len(fields) != 4:
        raise ValueError("expected four tab-separated fields")
    appid_text, count_text, positive_text, negative_text = fields
    if not appid_text.isdigit() or int(appid_text) <= 0:
        raise ValueError("invalid appid")
    try:
        count = int(count_text)
        positive = int(positive_text)
        negative = int(negative_text)
    except ValueError as exc:
        raise ValueError("non-integer additive state") from exc
    if count <= 0 or positive < 0 or negative < 0:
        raise ValueError("invalid additive state")
    if positive + negative != count:
        raise ValueError("label counts do not reconcile")
    return str(int(appid_text)), count, positive, negative


def format_metric(
    appid: str,
    review_count: int,
    positive_reviews: int,
    negative_reviews: int,
) -> str:
    if positive_reviews + negative_reviews != review_count:
        raise ValueError("reducer label counts do not reconcile")
    recommendation_rate = positive_reviews / review_count
    return (
        f"{appid}\t{review_count}\t{positive_reviews}\t"
        f"{negative_reviews}\t{recommendation_rate:.12f}"
    )


def reduce_stream(
    input_stream: TextIO,
    output_stream: TextIO,
    error_stream: TextIO,
) -> int:
    current_appid: str | None = None
    review_count = 0
    positive_reviews = 0
    negative_reviews = 0

    for line_number, raw_line in enumerate(input_stream, start=1):
        if not raw_line.strip():
            _counter(error_stream, "BlankIntermediateLines")
            continue
        try:
            appid, count, positive, negative = parse_intermediate(raw_line)
        except ValueError as exc:
            _counter(error_stream, "MalformedIntermediateRecords")
            print(
                f"reducer line={line_number} rejected: {exc}",
                file=error_stream,
            )
            continue

        if current_appid is not None and appid < current_appid:
            _counter(error_stream, "UnsortedInput")
            print("reducer input is not sorted by appid", file=error_stream)
            return 1
        if current_appid is not None and appid != current_appid:
            print(
                format_metric(
                    current_appid,
                    review_count,
                    positive_reviews,
                    negative_reviews,
                ),
                file=output_stream,
            )
            review_count = 0
            positive_reviews = 0
            negative_reviews = 0

        current_appid = appid
        review_count += count
        positive_reviews += positive
        negative_reviews += negative

    if current_appid is not None:
        print(
            format_metric(
                current_appid,
                review_count,
                positive_reviews,
                negative_reviews,
            ),
            file=output_stream,
        )
    return 0


def main() -> int:
    return reduce_stream(sys.stdin, sys.stdout, sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
