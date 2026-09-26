#!/usr/bin/env bash
set -euo pipefail
cd /workspace/i1

echo "=== Step 1: Independent Python baseline ==="
python3 baseline.py sample_access_log.csv > baseline_run.tsv
cat baseline_run.tsv

echo
echo "=== Step 2: Local MapReduce simulation ==="
cat sample_access_log.csv \
  | python3 mapper.py \
  | sort \
  | python3 combiner.py \
  | sort \
  | python3 reducer.py \
  > local_mapreduce_run.tsv
cat local_mapreduce_run.tsv

echo
echo "=== Step 3: Validate ==="
diff -u baseline_run.tsv local_mapreduce_run.tsv

echo "PASS: local MapReduce output matches the independent baseline."
