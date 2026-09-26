#!/usr/bin/env python3
import csv
import sys
from collections import defaultdict


if len(sys.argv) != 2:
    print("Usage: python3 baseline.py <input.csv>", file=sys.stderr)
    sys.exit(2)

aggregates = defaultdict(lambda: [0, 0, 0])
rejected = 0

with open(sys.argv[1], newline="", encoding="utf-8") as f:
    for row in csv.reader(f):
        if len(row) != 6:
            rejected += 1
            continue

        _, _, _, path, status_text, bytes_text = row
        try:
            status = int(status_text)
            bytes_sent = int(bytes_text)
        except ValueError:
            rejected += 1
            continue

        if bytes_sent < 0:
            rejected += 1
            continue

        aggregates[path][0] += 1
        aggregates[path][1] += bytes_sent
        aggregates[path][2] += 1 if status >= 400 else 0

for path in sorted(aggregates):
    count, total_bytes, errors = aggregates[path]
    error_rate = 100.0 * errors / count if count else 0.0
    print(
        f"{path}\t{count}\t{total_bytes}\t{errors}\t{error_rate:.2f}"
    )

print(f"Rejected records: {rejected}", file=sys.stderr)
