#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$PROJECT_ROOT/scripts/load_project_env.sh" "$PROJECT_ROOT/.env"

CONTAINER="${KAFKA_CONTAINER:-steam-kafka}"
TOPIC="${KAFKA_PLAYER_COUNT_TOPIC:-steam_player_events}"
PARTITIONS="${KAFKA_PLAYER_COUNT_TOPIC_PARTITIONS:-${KAFKA_TOPIC_PARTITIONS:-3}}"
RETENTION_MS="${KAFKA_RETENTION_MS:-86400000}"
RETENTION_BYTES="${KAFKA_RETENTION_BYTES:-536870912}"
KAFKA_CLI="/opt/kafka/bin/kafka-topics.sh"
CONFIG_CLI="/opt/kafka/bin/kafka-configs.sh"
BOOTSTRAP="localhost:9092"

docker exec "$CONTAINER" "$KAFKA_CLI" \
  --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor 1 \
  --config "retention.ms=$RETENTION_MS" \
  --config "retention.bytes=$RETENTION_BYTES" \
  --config "compression.type=producer"

docker exec "$CONTAINER" "$CONFIG_CLI" \
  --bootstrap-server "$BOOTSTRAP" \
  --entity-type topics \
  --entity-name "$TOPIC" \
  --alter \
  --add-config "retention.ms=$RETENTION_MS,retention.bytes=$RETENTION_BYTES,compression.type=producer"

docker exec "$CONTAINER" "$KAFKA_CLI" \
  --bootstrap-server "$BOOTSTRAP" \
  --describe --topic "$TOPIC"
