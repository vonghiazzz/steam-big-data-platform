# Operations Runbook

Chạy các lệnh từ repository root:

```bash
cd /Users/tannghiavo/Documents/AllProjects/WEB/BigData/web-scraping/steam
source .venv/bin/activate
```

## 1. Runtime prerequisites

OrbStack/Docker daemon phải chạy. Workflow onboarding tự start bốn Hadoop
container đã tồn tại; nó không recreate hoặc sửa compose environment.

Kiểm tra:

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

Steam API phải truy cập được. Nếu ISP/DNS trả
`store.steampowered.com → 127.0.0.1`, hãy bật VPN hoặc sửa DNS trước khi crawl.

```bash
dscacheutil -q host -a name store.steampowered.com
curl -I 'https://store.steampowered.com/appreviews/440?json=1'
```

## 2. Scheduled discovery/onboarding

Kiểm tra kế hoạch an toàn, không gọi Steam và không thay đổi registry:

```bash
bash scripts/run_discovery_scheduler.sh --run-once --dry-run
```

Chạy một WEEKLY cycle khi scheduler báo `due=true`:

```bash
bash scripts/run_discovery_scheduler.sh --run-once
```

Scheduler thực hiện bounded rotating catalog refresh, policy, registry
reconciliation, ACTIVE-capacity guard và gọi workflow onboarding có lock. Với
`90 ACTIVE`, `STREAM_MAX_ACTIVE_GAMES=100` và policy tối đa 10 game/cycle,
capacity hiện tại là 10. State scheduler nằm tại:

```text
data/state/discovery/scheduler_v1.json
```

Muốn scheduler tự kiểm tra liên tục, chạy process không có `--run-once`:

```bash
bash scripts/run_discovery_scheduler.sh
```

Process này phải được giữ chạy. OS boot auto-start/process supervision là phần
vận hành riêng.

## 3. Dynamic onboarding thủ công

Chạy discovery trên probe hiện có và xử lý tối đa 10 game theo policy:

```bash
bash scripts/run_onboarding_workflow.sh --run-discovery
```

Flow:

```text
Runtime → Discovery → Prepare → Crawl → Validate
→ Publish/Verify Bronze → Silver → Gold → Analytics
→ MongoDB → Readiness → ACTIVE
```

State nằm tại:

```text
data/onboarding/<batch-id>/workflow_state.json
```

Nếu batch lỗi, dùng batch ID được in ra để resume:

```bash
bash scripts/run_onboarding_workflow.sh --batch-id <batch-id>
```

Không chạy hai workflow đồng thời. Lock file sẽ từ chối concurrent run.

Muốn lấy cửa sổ Steam Store Search mới trước khi onboard:

```bash
bash scripts/run_onboarding_workflow.sh --run-discovery --refresh-catalog
```

Không chạy lặp nhiều cycle để vượt safety limit. Capacity guard giữ tổng ACTIVE
không vượt `STREAM_MAX_ACTIVE_GAMES`; các game vượt capacity vẫn ở trạng thái
`NEW`. Các entry `QUEUED` hiện hữu được giữ nguyên và chiếm capacity trước.

Flow:

```text
Catalog cursor → Policy → Registry → Capacity guard → QUEUED
→ Crawl → Bronze → Silver → Gold → Analytics → MongoDB → ACTIVE
```

Snapshot runtime đã xác nhận là `90 ACTIVE / 82 NEW / 0 QUEUED`, 45.000
historical reviews. Chỉ nâng safety limit sau khi đánh giá request budget và
producer cycle duration.

## 4. Kiểm tra onboarding

Registry:

```bash
python3 - <<'PY'
import json
from collections import Counter
from pathlib import Path

rows = [json.loads(line) for line in Path(
    "data/raw/registry/game_registry.jsonl"
).read_text().splitlines() if line.strip()]
print(Counter(row["status"] for row in rows))
PY
```

Queue sau batch thành công:

```bash
wc -l data/raw/registry/onboarding_queue.jsonl
```

MongoDB historical:

```bash
docker exec steam-mongodb mongosh --quiet --eval \
  'db.getSiblingDB("steam_analytics").game_metrics.countDocuments({})'
```

HDFS historical review file count:

```bash
docker exec bda501-namenode hdfs dfs -ls /steam/bronze/reviews |
  awk '$1 ~ /^-/ {count++} END {print count}'
```

## 5. Start Kafka và MongoDB

```bash
docker compose -f compose.streaming.yaml up -d
bash scripts/create_streaming_topic.sh
bash scripts/create_player_count_topic.sh

docker compose -f compose.mongodb.yaml up -d
```

## 6. Start Structured Streaming

Terminal 1:

Streaming checkpoint và `_spark_metadata` lưu URI HDFS tuyệt đối. Phải dùng một
authority ổn định trong mọi lần chạy. Project dùng:

```text
hdfs://bda501-namenode.orb.local:8020
```

Kiểm tra hostname trước:

```bash
nc -vz bda501-namenode.orb.local 8020
```

Nếu container IP thay đổi, cập nhật mapping `/etc/hosts`; không đổi
`HDFS_DEFAULT_FS` sang IP vì trộn hostname/IP trong metadata sẽ gây
`CONFLICTING_DIRECTORY_STRUCTURES`.

```bash
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
export HADOOP_USER_NAME=hadoop
export HDFS_DEFAULT_FS="hdfs://bda501-namenode.orb.local:8020"

MONGO_REALTIME_ENABLED=true \
STREAM_AVAILABLE_NOW=false \
bash scripts/run_review_streaming.sh 2>&1 | \
  tee /tmp/steam-review-streaming.log
```

`Unable to load native-hadoop library` và `Using incubator modules` là warning,
không phải lỗi. Process phải tiếp tục chờ micro-batch và không có traceback.

## 7. Start Review Producer

Terminal 2:

```bash
bash scripts/run_review_producer.sh
```

Đầu log phải khớp registry, ví dụ snapshot hiện tại:

```text
active_game_count=90
poll_scope_count=90
```

Review Producer reload ACTIVE registry ở đầu mỗi polling cycle. Game vừa được
activate sẽ được bootstrap historical recommendation IDs rồi tự tham gia scope;
không cần restart producer và không phát lại 500 historical reviews vào Kafka.
Spark Streaming cũng không cần restart.

## 7.1 Start Player Count Streaming và Producer

Terminal 3 chạy consumer liên tục, dùng cùng HDFS authority ổn định:

```bash
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
export HADOOP_USER_NAME=hadoop
export HDFS_DEFAULT_FS="hdfs://bda501-namenode.orb.local:8020"

MONGO_PLAYER_COUNT_ENABLED=true \
PLAYER_COUNT_AVAILABLE_NOW=false \
bash scripts/run_player_count_streaming.sh 2>&1 | \
  tee /tmp/steam-player-count-streaming.log
```

Terminal 4 chạy producer liên tục:

```bash
bash scripts/run_player_count_producer.sh
```

Producer mặc định poll mỗi 600 giây, delay khoảng một giây giữa request và
reload ACTIVE registry mỗi cycle. Smoke test giới hạn, chạy một lần:

```bash
bash scripts/run_player_count_producer.sh --once --max-games 2
```

Không chạy đồng thời hai player-count producer dùng cùng state DB.

## 8. Kiểm tra process và container

```bash
ps -ax -o pid=,etime=,command= |
  grep -E 'src.streaming.review_producer|src/streaming/review_streaming.py' |
  grep -v grep

docker ps --format 'table {{.Names}}\t{{.Status}}' |
  grep -E 'steam-|bda501-'
```

## 9. Kiểm tra MongoDB realtime

```bash
docker exec steam-mongodb mongosh --quiet steam_analytics --eval '
printjson({
  recent_reviews: db.recent_reviews.countDocuments({}),
  distinct_review_ids: db.recent_reviews.distinct("recommendationid").length,
  realtime_game_metrics: db.realtime_game_metrics.countDocuments({}),
  invalid_rates: db.realtime_game_metrics.countDocuments({
    $or: [
      {recommendation_rate: {$lt: 0}},
      {recommendation_rate: {$gt: 1}}
    ]
  })
})
'
```

Player count latest:

```bash
docker exec steam-mongodb mongosh --quiet steam_analytics --eval '
printjson({
  player_count_latest: db.player_count_latest.countDocuments({}),
  invalid_player_counts: db.player_count_latest.countDocuments({
    player_count: {$lt: 0}
  })
})
db.player_count_latest.find(
  {},
  {_id: 0, appid: 1, game_name: 1, player_count: 1, observed_at: 1}
).sort({observed_at: -1}).limit(20).forEach(printjson)
'
```

Kết quả tốt:

```text
recent_reviews == distinct_review_ids
invalid_rates == 0
```

Review mới nhất:

```bash
docker exec steam-mongodb mongosh --quiet steam_analytics --eval '
db.recent_reviews.find(
  {},
  {
    _id: 0,
    appid: 1,
    recommendationid: 1,
    voted_up: 1,
    timestamp_created: 1,
    stream_ingested_at: 1
  }
).sort({stream_ingested_at: -1}).limit(20).forEach(printjson)
'
```

## 10. Kiểm tra producer state và Kafka

```bash
sqlite3 -readonly data/state/streaming/review_producer_v1.sqlite3 '
SELECT appid, initialized, high_water_timestamp, last_polled_at
FROM game_state
ORDER BY last_polled_at DESC
LIMIT 20;
'

docker exec steam-kafka \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic steam_events

sqlite3 -readonly data/state/streaming/player_count_producer_v1.sqlite3 '
SELECT appid, last_player_count, last_observed_at, last_polled_at
FROM game_state
ORDER BY last_polled_at DESC
LIMIT 20;
'

docker exec steam-kafka \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic steam_player_events
```

Nếu Kafka end offset tăng sau một producer cycle, producer đã publish event.

## 11. Kiểm tra HDFS incremental

```bash
docker exec bda501-namenode hdfs dfs -ls -R \
  /steam/bronze/stream_events | tail -n 20

docker exec bda501-namenode hdfs dfs -ls -R \
  /steam/silver/reviews_incremental_v1 | tail -n 20

docker exec bda501-namenode hdfs dfs -ls -R \
  /steam/gold/base_incremental_v1 | tail -n 20

docker exec bda501-namenode hdfs dfs -ls -R \
  /steam/bronze/player_count_events | tail -n 20

docker exec bda501-namenode hdfs dfs -ls -R \
  /steam/silver/player_count_snapshots_v1 | tail -n 20

docker exec bda501-namenode hdfs dfs -ls -R \
  /steam/gold/player_count_snapshots_v1 | tail -n 20
```

## 12. Chạy MLlib V1 khi cần retrain

MLlib là batch retraining trên snapshot động `/steam/gold/base`, không phải
online learning:

```bash
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"

spark-submit --master 'local[2]' \
  --conf "spark.hadoop.fs.defaultFS=$HDFS_DEFAULT_FS" \
  src/ml/run_mllib_v1.py
```

## 13. Các giới hạn vận hành hiện tại

- WEEKLY scheduler đã có, nhưng process supervisor/OS auto-start chưa có.
- Review producer và player-count producer đều reload ACTIVE registry mỗi cycle.
- Review Streaming V1 chỉ hỗ trợ `REVIEW_CREATED`; không theo dõi vote update.
- Player Count Streaming V1 đã triển khai riêng với
  `PLAYER_COUNT_SNAPSHOT`, `steam_player_events` và `player_count_latest`.
- Price/metadata/review-update vẫn mới là thiết kế. Xem
  [Near-real-time, Streaming và Batch](03_NEAR_REALTIME_VS_BATCH.md).
- Catalog refresh dùng bounded rotating cursor, không crawl toàn bộ Steam trong
  một lần chạy.
- Khi Steam API bị chặn, workflow fail ở crawl và có thể resume sau khi mạng
  hoạt động lại.
