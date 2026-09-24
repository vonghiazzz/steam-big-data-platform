#!/usr/bin/env bash
# Upload the Steam Bronze handoff (games + reviews) to HDFS Bronze.
# Run from the repository ROOT:   bash src/hdfs/upload_bronze.sh
#
# Local input  (after unzip + move, see guide):
#   data/raw/steam/landing/games/games_raw.jsonl
#   data/raw/steam/bronze_ready/reviews_by_game/<appid>.jsonl   (50 files)
# HDFS target:
#   /steam/bronze/games/games_raw.jsonl
#   /steam/bronze/reviews/<appid>.jsonl
#
# Bronze is raw + immutable, so this script REFUSES to overwrite existing
# files. To redo a bad upload on purpose:  FORCE=1 bash src/hdfs/upload_bronze.sh
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: stop it rewriting /tmp, /steam paths

CONTAINER="${CONTAINER:-bda501-namenode}"
LOCAL_ROOT="${LOCAL_ROOT:-data/raw/steam}"
GAMES_SRC="$LOCAL_ROOT/landing/games/games_raw.jsonl"
REVIEWS_SRC="$LOCAL_ROOT/bronze_ready/reviews_by_game"
HDFS_GAMES="/steam/bronze/games"
HDFS_REVIEWS="/steam/bronze/reviews"
STAGE="/tmp/steam_handoff"                     # temp dir INSIDE the container
EVIDENCE="${EVIDENCE:-evidence/hdfs/bronze_inventory.txt}"

dx()  { docker exec "$CONTAINER" "$@"; }                 # default container user (hdfs/hadoop commands)
dxr() { docker exec -u root "$CONTAINER" "$@"; }        # root: staging dir only. `docker cp` creates files
                                                        # owned by root, which the default user cannot delete.

echo "== 1. Preflight =="
[[ -f "$GAMES_SRC" ]]   || { echo "Missing $GAMES_SRC";   exit 1; }
[[ -d "$REVIEWS_SRC" ]] || { echo "Missing $REVIEWS_SRC"; exit 1; }
n_local=$(ls "$REVIEWS_SRC"/*.jsonl | wc -l | tr -d ' ')
echo "local review files: $n_local (expected 50)"
[[ "$n_local" == "50" ]] || { echo "Unexpected local file count"; exit 1; }
docker ps --format '{{.Names}}' | grep -qx "$CONTAINER" \
  || { echo "Container '$CONTAINER' is not running (docker ps to see names; CONTAINER=<name> to override)"; exit 1; }
echo "waiting for NameNode to leave safe mode (max 180s; normal right after containers start)..."
for i in $(seq 1 36); do
  if dx hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; then echo "Safe mode is OFF"; break; fi
  if [[ "$i" == "36" ]]; then
    echo "Still in safe mode after 180s. Check: docker exec $CONTAINER hdfs dfsadmin -report  (is the datanode live?)"
    exit 1
  fi
  sleep 5
done

echo "== 2. Guard: do not overwrite Bronze =="
if dx hdfs dfs -test -e "$HDFS_REVIEWS/570.jsonl" 2>/dev/null || dx hdfs dfs -test -e "$HDFS_GAMES/games_raw.jsonl" 2>/dev/null; then
  if [[ "${FORCE:-0}" != "1" ]]; then
    echo "Bronze files already exist. Re-run with FORCE=1 to delete and re-upload."; exit 1
  fi
  echo "FORCE=1 -> removing existing Bronze games/reviews"
  dx hdfs dfs -rm -r -f "$HDFS_GAMES" "$HDFS_REVIEWS"
fi

echo "== 3. Copy local files into the container staging dir =="
dxr rm -rf "$STAGE"
dxr mkdir -p "$STAGE"
docker cp "$GAMES_SRC"       "$CONTAINER:$STAGE/games_raw.jsonl"
docker cp "$REVIEWS_SRC/."   "$CONTAINER:$STAGE/reviews"
dxr chmod -R a+rX "$STAGE"                      # make sure the default user can read what hdfs -put uploads

echo "== 4. Create HDFS Bronze paths and upload =="
dx hdfs dfs -mkdir -p "$HDFS_GAMES" "$HDFS_REVIEWS"
dx hdfs dfs -put "$STAGE/games_raw.jsonl" "$HDFS_GAMES/"
dx sh -c "hdfs dfs -put $STAGE/reviews/*.jsonl $HDFS_REVIEWS/"

echo "== 5. Clean staging =="
dxr rm -rf "$STAGE"

echo "== 6. Save raw inventory evidence -> $EVIDENCE =="
mkdir -p "$(dirname "$EVIDENCE")"
{
  echo "# HDFS Bronze inventory  ($(date -u +%FT%TZ), container=$CONTAINER)"
  for cmd in \
    "hdfs dfs -ls -h /steam/bronze/games" \
    "hdfs dfs -ls -h /steam/bronze/reviews" \
    "hdfs dfs -count -v /steam/bronze/games /steam/bronze/reviews" \
    "hdfs dfs -du -s -h /steam/bronze/games /steam/bronze/reviews" \
    "hdfs fsck /steam/bronze -files -blocks | tail -25"; do
    echo; echo "\$ $cmd"
    dx sh -c "$cmd"
  done
} | tee "$EVIDENCE"

echo; echo "Upload done. Next: python src/hdfs/verify_bronze_integrity.py --source hdfs --out evidence/hdfs/bronze_verification.txt"