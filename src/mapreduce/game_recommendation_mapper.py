#!/usr/bin/env python3
"""Map canonical Steam review JSONL to additive per-game counts."""

import json
import os
import re
import sys
from typing import TextIO


APPID_FILE_PATTERN = re.compile(r"(?:^|/)([0-9]+)\.jsonl(?:$|[?#])")
COUNTER_GROUP = "Steam MapReduce Cross-check"


class RecordError(ValueError):
    """A source row cannot be mapped without changing its semantics."""


def _normalize_appid(value: object) -> str:
    if isinstance(value, bool):
        raise RecordError("invalid appid")
    if isinstance(value, int):
        appid = value
    elif isinstance(value, str) and value.isdigit():
        appid = int(value)
    else:
        raise RecordError("invalid appid")
    if appid <= 0:
        raise RecordError("invalid appid")
    return str(appid)


def appid_from_input_path(input_path: str | None) -> str:
    if not input_path:
        raise RecordError("missing appid and input path")
    match = APPID_FILE_PATTERN.search(input_path)
    if not match:
        raise RecordError("cannot derive appid from input path")
    return _normalize_appid(match.group(1))


def parse_review_line(
    raw_line: str,
    input_path: str | None = None,
) -> tuple[str, int, int, int]:
    try:
        record = json.loads(raw_line)
    except json.JSONDecodeError as exc:
        raise RecordError("malformed JSON") from exc
    if not isinstance(record, dict):
        raise RecordError("JSON record is not an object")

    if "appid" in record and record["appid"] is not None:
        appid = _normalize_appid(record["appid"])
    else:
        appid = appid_from_input_path(input_path)

    review = record.get("review")
    if not isinstance(review, dict):
        review = record
    voted_up = review.get("voted_up")
    if not isinstance(voted_up, bool):
        raise RecordError("voted_up is not boolean")

    positive = int(voted_up)
    negative = int(not voted_up)
    return appid, 1, positive, negative


def _input_path_from_environment() -> str | None:
    for name in (
        "mapreduce_map_input_file",
        "map_input_file",
        "mapreduce.map.input.file",
    ):
        value = os.environ.get(name)
        if value:
            return value
    return None


def _counter(error_stream: TextIO, name: str) -> None:
    print(f"reporter:counter:{COUNTER_GROUP},{name},1", file=error_stream)


def map_stream(
    input_stream: TextIO,
    output_stream: TextIO,
    error_stream: TextIO,
    input_path: str | None = None,
) -> int:
    source_path = input_path or _input_path_from_environment()
    for line_number, raw_line in enumerate(input_stream, start=1):
        if not raw_line.strip():
            _counter(error_stream, "BlankLines")
            continue
        try:
            appid, count, positive, negative = parse_review_line(
                raw_line,
                source_path,
            )
        except RecordError as exc:
            _counter(error_stream, "MalformedRecords")
            print(
                f"mapper line={line_number} rejected: {exc}",
                file=error_stream,
            )
            continue
        print(
            f"{appid}\t{count}\t{positive}\t{negative}",
            file=output_stream,
        )
    return 0


def main() -> int:
    return map_stream(sys.stdin, sys.stdout, sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
