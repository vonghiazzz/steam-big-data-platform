#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
MONGO_URI=${MONGO_URI:-mongodb://localhost:27017}
MONGO_DATABASE=${MONGO_DATABASE:-steam_analytics}
MONGODB_CONTAINER=${MONGODB_CONTAINER:-steam-mongodb}
NAMENODE_CONTAINER=${HADOOP_NAMENODE_CONTAINER:-bda501-namenode}
PYTHON_BIN=${PYTHON_BIN:-python3}

SPARK_SUBMIT=${SPARK_SUBMIT:-$(command -v spark-submit || true)}
if [[ -z "$SPARK_SUBMIT" ]]; then
  echo "spark-submit is unavailable; set SPARK_SUBMIT explicitly" >&2
  exit 1
fi
if ! "$PYTHON_BIN" -c "import pymongo" >/dev/null 2>&1; then
  echo "PyMongo is unavailable for $PYTHON_BIN; install requirements.txt" >&2
  exit 1
fi

docker compose -f "$PROJECT_ROOT/compose.mongodb.yaml" up -d

for _ in $(seq 1 30); do
  health=$(
    docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' \
      "$MONGODB_CONTAINER" 2>/dev/null || true
  )
  if [[ "$health" == "healthy" ]]; then
    break
  fi
  sleep 2
done
if [[ "${health:-}" != "healthy" ]]; then
  echo "MongoDB did not become healthy" >&2
  exit 1
fi

HDFS_DEFAULT_FS=${HDFS_DEFAULT_FS:-}
if [[ -z "$HDFS_DEFAULT_FS" ]]; then
  namenode_ip=$(
    docker inspect -f \
      '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
      "$NAMENODE_CONTAINER"
  )
  if [[ -z "$namenode_ip" ]]; then
    echo "Could not resolve NameNode container IP" >&2
    exit 1
  fi
  HDFS_DEFAULT_FS="hdfs://$namenode_ip:8020"
fi

PYTHONPATH="$PROJECT_ROOT/src${PYTHONPATH:+:$PYTHONPATH}" \
PYTHONDONTWRITEBYTECODE=1 \
"$SPARK_SUBMIT" \
  --master 'local[2]' \
  --conf spark.ui.enabled=false \
  --conf "spark.hadoop.fs.defaultFS=$HDFS_DEFAULT_FS" \
  --conf spark.hadoop.dfs.client.use.datanode.hostname=false \
  "$PROJECT_ROOT/src/serving/run_mongodb_serving.py" \
  --mongo-uri "$MONGO_URI" \
  --database "$MONGO_DATABASE" \
  --evidence-dir "$PROJECT_ROOT/evidence/serving"
