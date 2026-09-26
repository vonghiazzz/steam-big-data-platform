#!/usr/bin/env python3
import sys


def emit(key, count, total_bytes, errors):
    if key is None:
        return
    error_rate = 100.0 * errors / count if count else 0.0
    print(
        f"{key}\t{count}\t{total_bytes}\t{errors}\t{error_rate:.2f}"
    )


current_key = None
count = 0
total_bytes = 0
errors = 0

for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue

    key, value = line.split("\t", 1)
    req_count, bytes_sent, error_count = map(int, value.split(","))

    if current_key is not None and key != current_key:
        emit(current_key, count, total_bytes, errors)
        count = 0
        total_bytes = 0
        errors = 0

    current_key = key
    count += req_count
    total_bytes += bytes_sent
    errors += error_count

emit(current_key, count, total_bytes, errors)
