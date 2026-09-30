#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$PROJECT_ROOT/scripts/load_project_env.sh" "$PROJECT_ROOT/.env"
SPARK_KAFKA_PACKAGE="${SPARK_KAFKA_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0}"

cd "$PROJECT_ROOT"
export PYTHONPATH="$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"
export HADOOP_USER_NAME="${HADOOP_USER_NAME:-hadoop}"

case "${MONGO_REALTIME_ENABLED:-false}" in
  1|true|TRUE|yes|YES)
    echo "MongoDB realtime serving: ENABLED"
    ;;
  *)
    echo "MongoDB realtime serving: DISABLED"
    ;;
esac

exec spark-submit \
  --packages "$SPARK_KAFKA_PACKAGE" \
  src/streaming/review_streaming.py "$@"
