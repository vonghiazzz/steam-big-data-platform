# Data Pipelines

## Historical batch và dynamic onboarding

```text
Qualified game
→ QUEUED registry entry
→ 500 historical reviews/game
→ isolated staging validation
→ /steam/bronze/games/onboarding-<batch-id>.jsonl
→ /steam/bronze/reviews/<appid>.jsonl
→ Spark Silver
→ Gold Base
→ nine Gold Analytics datasets
→ MongoDB historical collections
→ readiness PASS
→ ACTIVE
```

Một batch mặc định tối đa 10 game, tức 5.000 review. Snapshot hiện tại gồm 60
game và 30.000 historical review. Silver, Gold, Analytics và MongoDB được
rebuild từ toàn bộ canonical Bronze sau khi batch mới được publish.

## Realtime review pipeline

```text
ACTIVE Game Registry
→ Review Producer polls Steam every 10 minutes
→ REVIEW_CREATED event
→ Kafka topic steam_events
→ Spark Structured Streaming micro-batch
   ├─ /steam/bronze/stream_events
   ├─ /steam/quarantine/review_events
   ├─ /steam/silver/reviews_incremental_v1
   ├─ /steam/gold/base_incremental_v1
   ├─ MongoDB recent_reviews
   └─ MongoDB realtime_game_metrics
```

Khoảng nghỉ khoảng một giây là delay giữa API requests, không phải tần suất
poll toàn bộ registry. Spark xử lý Kafka theo micro-batch khoảng một phút.

Producer bootstrap review IDs lịch sử từ HDFS nên không phát lại 500 review
backfill của game vừa onboarding. Nó chỉ emit review chưa từng thấy.

## Các layer

- Bronze: raw và replayable.
- Silver: clean, typed, validated, deduplicated.
- Gold Base: một row cho một review.
- Gold Analytics: `game_metrics`, `genre_metrics`, `playtime_metrics`,
  `free_paid_metrics`, `engagement_metrics`, `label_profile`,
  `platform_metrics`, `category_metrics`, `purchase_metrics`.

## Historical và realtime

Historical data là snapshot dùng để rebuild toàn bộ batch analytics. Realtime
data chỉ chứa review mới sau bootstrap và nằm ở các path/collection incremental
riêng. Hai nhánh cùng dùng schema, validation và serving concepts nhưng không
được mô tả như một physical Silver directory duy nhất.
