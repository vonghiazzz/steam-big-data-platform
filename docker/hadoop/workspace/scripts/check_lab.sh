#!/usr/bin/env bash
set -euo pipefail

echo "=== BDA501 Hadoop Lab Health Check ==="
echo
python3 --version
hadoop version | head -n 2
echo

echo "--- HDFS root ---"
hdfs dfs -ls /
echo

echo "--- HDFS DataNode report ---"
hdfs dfsadmin -report | sed -n '1,35p'
echo

echo "--- YARN nodes ---"
yarn node -list -all
echo

echo "--- Sample input in HDFS ---"
hdfs dfs -ls /user/bda501/i1/input
hdfs dfs -cat /user/bda501/i1/input/sample_access_log.csv | head -n 3
echo

echo "HEALTH CHECK COMPLETED."
