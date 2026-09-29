#!/usr/bin/env bash
set -euo pipefail

MIN_FREE_GB="${STREAM_MIN_FREE_GB:-5}"
PROJECT_PATH="${PROJECT_PATH:-$(pwd)}"
NAMENODE_CONTAINER="${HADOOP_NAMENODE_CONTAINER:-bda501-namenode}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-steam-kafka}"

echo "== Host filesystem =="
df -h "$PROJECT_PATH"

free_kb=$(df -Pk "$PROJECT_PATH" | awk 'NR==2 {print $4}')
min_kb=$((MIN_FREE_GB * 1024 * 1024))
if (( free_kb < min_kb )); then
  echo "WARNING: host free space is below ${MIN_FREE_GB} GiB" >&2
  host_status=2
else
  host_status=0
fi

echo
echo "== Docker storage =="
docker system df

echo
echo "== HDFS capacity and /steam usage =="
docker exec "$NAMENODE_CONTAINER" hdfs dfs -df -h /
docker exec "$NAMENODE_CONTAINER" hdfs dfs -du -h /steam

echo
echo "== Streaming checkpoint sizes =="
docker exec "$NAMENODE_CONTAINER" hdfs dfs -du -h /steam/checkpoints 2>/dev/null \
  || echo "No /steam/checkpoints path yet"

echo
echo "== Kafka log size =="
docker exec "$KAFKA_CONTAINER" du -sh /var/lib/kafka/data 2>/dev/null \
  || echo "Kafka container is not running"

exit "$host_status"
