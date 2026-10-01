# HDFS, Spark và MongoDB UI/Inspection

Repository chưa có application dashboard riêng. Các giao diện dưới đây dùng để
quan sát storage và jobs; nội dung Parquet nên đọc bằng Spark SQL.

## 1. HDFS NameNode UI

Mở:

```text
http://localhost:9870
```

Chọn `Utilities → Browse the file system`, hoặc mở:

```text
http://localhost:9870/explorer.html#/steam
```

Các path chính:

```text
/steam
├─ bronze
│  ├─ games
│  ├─ reviews
│  └─ stream_events
├─ silver
│  ├─ games
│  ├─ reviews
│  └─ reviews_incremental_v1
├─ gold
│  ├─ base
│  ├─ base_incremental_v1
│  └─ analytics
├─ quarantine
└─ checkpoints
```

NameNode UI cho biết file, size, owner, permission, replication, block, live/
dead DataNodes và capacity. Nó không hiển thị Parquet như một bảng.

## 2. Đọc Gold bằng PySpark

Khởi động PySpark từ repository root với endpoint NameNode hiện tại:

```bash
export HADOOP_USER_NAME=hadoop
export HDFS_DEFAULT_FS="hdfs://$(
  docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
    bda501-namenode
):8020"

pyspark \
  --master 'local[2]' \
  --conf "spark.hadoop.fs.defaultFS=$HDFS_DEFAULT_FS" \
  --conf spark.hadoop.dfs.client.use.datanode.hostname=false
```

Trong PySpark:

```python
gold = spark.read.parquet("/steam/gold/base")
print("gold rows:", gold.count())
print("games:", gold.select("appid").distinct().count())
gold.show(5, truncate=False)

game = spark.read.parquet("/steam/gold/analytics/game_metrics")
game.orderBy("recommendation_rate", ascending=False).show(10, truncate=False)

profile = spark.read.parquet("/steam/gold/analytics/label_profile")
profile.show(truncate=False)
```

Không hard-code kỳ vọng 50 rows. Snapshot ngày 2026-10-01 có 60 game và
30.000 Gold rows; số này tăng sau các onboarding batch tiếp theo.

## 3. Gold Analytics datasets

| Dataset | Câu hỏi chính |
|---|---|
| `game_metrics` | Metrics theo game |
| `genre_metrics` | Recommendation theo genre |
| `playtime_metrics` | Playtime bucket và recommendation |
| `free_paid_metrics` | Free và paid game |
| `engagement_metrics` | Playtime/vote engagement theo game |
| `label_profile` | Label distribution và data quality |
| `platform_metrics` | Windows/Mac/Linux |
| `category_metrics` | Steam categories |
| `purchase_metrics` | Steam purchase và nguồn khác |

## 4. Spark UI

Khi một Spark job đang chạy, mở:

```text
http://localhost:4040
```

Nếu port bận, Spark có thể dùng 4041, 4042, v.v. Spark UI chỉ tồn tại trong lúc
process còn chạy, trừ khi cấu hình History Server.

Các tab hữu ích:

- Jobs/Stages/Tasks: tiến độ và shuffle;
- SQL: physical plan, join và aggregate;
- Storage: cached datasets;
- Executors: CPU, memory và task statistics;
- Streaming: micro-batch progress nếu job hỗ trợ hiển thị.

## 5. MongoDB inspection

Historical serving:

```bash
docker exec steam-mongodb mongosh --quiet steam_analytics --eval '
printjson({
  games: db.game_metrics.countDocuments({}),
  historical_reviews: db.label_profile.findOne(
    {_id: "historical_baseline"}
  ).total_rows,
  recent_reviews: db.recent_reviews.countDocuments({}),
  realtime_windows: db.realtime_game_metrics.countDocuments({})
})
'
```

MongoDB là serving view. Dùng Spark/HDFS để kiểm tra analytical source of truth,
và dùng MongoDB để kiểm tra dữ liệu mà API/UI sẽ đọc.
