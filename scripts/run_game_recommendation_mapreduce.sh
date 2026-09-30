#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
NAMENODE_CONTAINER=${HADOOP_NAMENODE_CONTAINER:-bda501-namenode}
INPUT_PATH='/steam/bronze/reviews/*.jsonl'
OUTPUT_PATH='/steam/mapreduce/game_recommendation_metrics'
SPARK_REFERENCE_PATH='/steam/gold/analytics/game_metrics'
EVIDENCE_DIR="$PROJECT_ROOT/evidence/mapreduce"
CONTAINER_WORK_DIR='/tmp/steam-mapreduce-crosscheck'
MAPPER_NAME='game_recommendation_mapper.py'
REDUCER_NAME='game_recommendation_reducer.py'

mkdir -p "$EVIDENCE_DIR"

STREAMING_JAR=$(
  docker exec "$NAMENODE_CONTAINER" sh -lc \
    "find /opt/hadoop/share/hadoop/tools/lib -type f -name 'hadoop-streaming-*.jar' | sort | head -n 1"
)
if [[ -z "$STREAMING_JAR" ]]; then
  echo "Hadoop Streaming JAR was not found" >&2
  exit 1
fi

docker exec "$NAMENODE_CONTAINER" mkdir -p "$CONTAINER_WORK_DIR"
docker cp \
  "$PROJECT_ROOT/src/mapreduce/$MAPPER_NAME" \
  "$NAMENODE_CONTAINER:$CONTAINER_WORK_DIR/$MAPPER_NAME"
docker cp \
  "$PROJECT_ROOT/src/mapreduce/$REDUCER_NAME" \
  "$NAMENODE_CONTAINER:$CONTAINER_WORK_DIR/$REDUCER_NAME"

if docker exec "$NAMENODE_CONTAINER" hdfs dfs -test -e "$OUTPUT_PATH"; then
  docker exec "$NAMENODE_CONTAINER" hdfs dfs -rm -r "$OUTPUT_PATH"
fi

{
  echo "hadoop_version=$(docker exec "$NAMENODE_CONTAINER" hadoop version | head -n 1)"
  echo "streaming_jar=$STREAMING_JAR"
  echo "input_path=$INPUT_PATH"
  echo "output_path=$OUTPUT_PATH"
  echo "reducers=1"
} > "$EVIDENCE_DIR/job_summary.txt"

docker exec "$NAMENODE_CONTAINER" hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name=steam-game-recommendation-crosscheck-v1 \
  -D mapreduce.job.reduces=1 \
  -files \
    "$CONTAINER_WORK_DIR/$MAPPER_NAME,$CONTAINER_WORK_DIR/$REDUCER_NAME" \
  -mapper "python3 $MAPPER_NAME" \
  -reducer "python3 $REDUCER_NAME" \
  -input "$INPUT_PATH" \
  -output "$OUTPUT_PATH" \
  2>&1 | tee -a "$EVIDENCE_DIR/job_summary.txt"

{
  docker exec "$NAMENODE_CONTAINER" hdfs dfs -ls -h "$OUTPUT_PATH"
  docker exec "$NAMENODE_CONTAINER" hdfs dfs -count -v "$OUTPUT_PATH"
} > "$EVIDENCE_DIR/hdfs_output_listing.txt"

docker exec "$NAMENODE_CONTAINER" \
  hdfs dfs -cat "$OUTPUT_PATH/part-*" \
  > "$EVIDENCE_DIR/mapreduce_output.tsv"

SPARK_SUBMIT=${SPARK_SUBMIT:-$(command -v spark-submit || true)}
if [[ -z "$SPARK_SUBMIT" ]]; then
  echo "spark-submit is required to read the Spark reference Parquet" >&2
  exit 1
fi
NAMENODE_IP=$(
  docker inspect -f \
    '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
    "$NAMENODE_CONTAINER"
)
if [[ -z "$NAMENODE_IP" ]]; then
  echo "Could not determine the NameNode container IP" >&2
  exit 1
fi

PYTHONPATH="$PROJECT_ROOT/src${PYTHONPATH:+:$PYTHONPATH}" \
PYTHONDONTWRITEBYTECODE=1 \
"$SPARK_SUBMIT" \
  --master 'local[2]' \
  --conf spark.ui.enabled=false \
  --conf "spark.hadoop.fs.defaultFS=hdfs://$NAMENODE_IP:8020" \
  --conf spark.hadoop.dfs.client.use.datanode.hostname=false \
  "$PROJECT_ROOT/src/mapreduce/compare_game_metrics.py" \
  --mapreduce-output "$EVIDENCE_DIR/mapreduce_output.tsv" \
  --spark-reference-hdfs "$SPARK_REFERENCE_PATH" \
  --spark-reference-output "$EVIDENCE_DIR/spark_reference.tsv" \
  --comparison-output "$EVIDENCE_DIR/comparison.txt"

echo
echo "Evidence created in: $EVIDENCE_DIR"
echo "HDFS output: $OUTPUT_PATH"
