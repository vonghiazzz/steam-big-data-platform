#!/usr/bin/env python3
import csv
import sys


def reject(reason: str) -> None:
    print(
        f"reporter:counter:BDA501 Input,{reason},1",
        file=sys.stderr,
    )


for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue

    try:
        row = next(csv.reader([line]))
    except csv.Error:
        reject("CSVParseError")
        continue

    if len(row) != 6:
        reject("WrongColumnCount")
        continue

    _, _, _, path, status_text, bytes_text = row

    try:
        status = int(status_text)
        bytes_sent = int(bytes_text)
    except ValueError:
        reject("InvalidNumericField")
        continue

    if bytes_sent < 0:
        reject("NegativeBytes")
        continue

    error_flag = 1 if status >= 400 else 0

    # key = endpoint/path
    # value = request_count,total_bytes,error_count
    print(f"{path}\t1,{bytes_sent},{error_flag}")
