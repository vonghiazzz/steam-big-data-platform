#!/usr/bin/env bash
set -euo pipefail
cd /workspace/i1

INPUT=/user/bda501/i1/input/sample_access_log.csv
OUTPUT=/user/bda501/i1/output
STREAMING_JAR="$(find /opt/hadoop/share/hadoop/tools/lib -maxdepth 1 -name 'hadoop-streaming-*.jar' | head -n 1)"

if [ -z "$STREAMING_JAR" ]; then
  echo "ERROR: Hadoop Streaming JAR not found." >&2
  exit 1
fi

echo "=== BDA501 Individual 1 Worked Example ==="
echo "Streaming JAR: $STREAMING_JAR"
echo

echo "[1/5] Running local validation first..."
bash /workspace/scripts/run_i1_local.sh

echo
echo "[2/5] Ensuring the input is available in HDFS..."
hdfs dfs -mkdir -p /user/bda501/i1/input
hdfs dfs -put -f sample_access_log.csv "$INPUT"
hdfs dfs -ls /user/bda501/i1/input

echo
echo "[3/5] Removing previous Hadoop output, if any..."
hdfs dfs -rm -r -f "$OUTPUT" >/dev/null 2>&1 || true

echo
echo "[4/5] Submitting Hadoop Streaming job to YARN..."
hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name="BDA501-I1-Web-Access-Log-Analytics" \
  -D mapreduce.job.reduces=1 \
  -files mapper.py,combiner.py,reducer.py \
  -mapper "python3 mapper.py" \
  -combiner "python3 combiner.py" \
  -reducer "python3 reducer.py" \
  -input "$INPUT" \
  -output "$OUTPUT"

echo
echo "[5/5] Reading and validating HDFS output..."
hdfs dfs -cat "$OUTPUT/part-*" > hadoop_output.tsv
cat hadoop_output.tsv

diff -u expected_output.tsv hadoop_output.tsv

echo
cat <<'MSG'
PASS: Hadoop Streaming output matches expected_output.tsv.

Now inspect:
  NameNode UI       http://localhost:9870
  ResourceManager   http://localhost:8088
  JobHistory        http://localhost:19888

Useful commands:
  hdfs dfs -ls /user/bda501/i1/output
  hdfs dfs -cat /user/bda501/i1/output/part-*
  yarn application -list -appStates ALL
MSG
