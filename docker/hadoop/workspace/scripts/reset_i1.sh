#!/usr/bin/env bash
set -euo pipefail
hdfs dfs -rm -r -f /user/bda501/i1/output >/dev/null 2>&1 || true
rm -f /workspace/i1/baseline_run.tsv /workspace/i1/local_mapreduce_run.tsv /workspace/i1/hadoop_output.tsv
printf 'I1 output reset. Input remains in HDFS.\n'
