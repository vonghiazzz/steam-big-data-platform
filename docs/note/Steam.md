# Steam Big Data Platform — Luồng chi tiết

## 1. Nguồn dữ liệu

Nguồn chính của implementation hiện tại:

```text
Steam catalog/probe files
Steam appdetails metadata
Steam appreviews API
```

Hệ thống không crawl toàn bộ Steam vào HDFS. Discovery và policy quyết định
game nào được quản lý.

## 2. Discovery và qualification

```text
Candidate files
→ metadata probe
→ review availability probe
→ qualification policy
→ Registry reconciliation
→ onboarding queue
```

Policy V1 trong `config/discovery_policy.json` yêu cầu:

- `type = game`;
- release age ít nhất 30 ngày;
- ít nhất 1.000 total reviews;
- metadata hợp lệ;
- review endpoint hoạt động.

Operational policy:

- tối đa 10 game mới/cycle;
- backfill 500 English reviews/game;
- ba lần retry với exponential backoff;
- discovery frequency `WEEKLY` được thực thi bởi lightweight scheduler; process
  scheduler phải đang chạy hoặc được gọi bằng `--run-once`.

Policy áp dụng cho game onboarding mới. ACTIVE game không tự bị loại khi policy
thay đổi; re-evaluation là một operation riêng.

## 3. Game Registry

Registry là control plane, không phải danh sách cố định 50 game.

Các status:

```text
NEW → QUEUED → ACTIVE
             ↘ FAILED / PAUSED
ACTIVE → RETIRED
```

Snapshot đã kiểm chứng ngày 2026-10-01:

```text
60 ACTIVE
106 NEW
0 QUEUED
```

`selected_50_games.jsonl` là seed/baseline nghiên cứu ban đầu. Mười game mới đã
được onboarding thành công qua workflow động.

## 4. NEW/QUEUED game — historical onboarding

```text
Discovery
→ chọn tối đa 10 NEW
→ chuyển QUEUED
→ tạo immutable manifest
→ stage metadata
→ crawl 500 reviews/game
→ validate count/schema/ID/overlap
→ publish HDFS Bronze
→ rebuild Silver/Gold/Analytics
→ load MongoDB
→ per-game readiness
→ ACTIVE
```

Workflow được chạy bằng:

```bash
bash scripts/run_onboarding_workflow.sh --run-discovery
```

Nó có lock, phase state và resume. File đã publish vào Bronze chỉ được reuse khi
nội dung giống staging; không overwrite raw object khác nội dung.

Batch đầu tiên đã mở rộng historical snapshot:

```text
50 games / 25.000 reviews
→ 60 games / 30.000 reviews
```

Mỗi game mới có đủ 500 rows tại Bronze, Silver và Gold, đồng thời có MongoDB
`game_metrics`, trước khi Registry chuyển sang ACTIVE.

## 5. ACTIVE game — realtime reviews

Implementation realtime V1 chỉ hỗ trợ review mới:

```text
ACTIVE Registry
→ Review Producer
→ poll Steam appreviews API
→ compare recommendationid với SQLite state
→ REVIEW_CREATED
→ Kafka steam_events
```

Producer bootstrap historical IDs từ HDFS nên 500 review backfill không bị emit
lại. Nó poll toàn bộ scope khoảng 10 phút/lần và nghỉ khoảng một giây giữa API
requests.

Producer nạp ACTIVE scope khi process khởi động. Sau onboarding phải restart
producer để thêm game vừa ACTIVE. Hot-reload registry chưa được triển khai.

Các event sau chưa có trong V1:

```text
PRICE_UPDATED
GAME_METADATA_UPDATED
REVIEW_VOTE_UPDATED
PLAYER_COUNT_UPDATED
```

## 6. Kafka và Structured Streaming

```text
Kafka steam_events
→ Spark Structured Streaming
→ parse schema
→ quarantine invalid records
→ event-time/watermark
→ deduplicate event_id
├─ HDFS Bronze event archive
├─ incremental Silver
├─ incremental Gold
├─ MongoDB recent_reviews
└─ MongoDB realtime_game_metrics
```

Kafka là transport/distributed log, không thay HDFS hoặc MongoDB. Spark dùng
checkpoint để giữ offset và streaming state. `availableNow=false` chạy liên
tục; micro-batch mặc định khoảng một phút.

## 7. HDFS layers

```text
/steam/bronze/games
/steam/bronze/reviews
/steam/bronze/stream_events
/steam/silver/games
/steam/silver/reviews
/steam/silver/reviews_incremental_v1
/steam/gold/base
/steam/gold/base_incremental_v1
/steam/gold/analytics/*
/steam/quarantine/review_events
/steam/checkpoints/*
```

### Bronze

Raw, immutable, replayable. Bronze chỉ giới hạn scope theo Registry; không thực
hiện analytical transformation.

### Silver

Typed, cleaned, normalized, validated và deduplicated. Historical Silver và
incremental review Silver hiện dùng path riêng.

### Gold Base

Một row cho một review, join với game metadata và bổ sung các trường analytics/
ML như label, playtime bucket, price, genres, categories và platforms.

### Gold Analytics

Chín datasets:

```text
game_metrics
genre_metrics
playtime_metrics
free_paid_metrics
engagement_metrics
label_profile
platform_metrics
category_metrics
purchase_metrics
```

Snapshot hiện tại:

```text
30.000 Gold rows
60 distinct appids
22.825 positive
7.175 negative
```

## 8. MongoDB serving

Historical Gold Analytics được load idempotently vào database
`steam_analytics`. Natural keys dùng làm `_id`; reload upsert snapshot mới và
xóa stale documents của snapshot cũ.

Realtime dùng collection riêng:

```text
recent_reviews
realtime_game_metrics
```

MongoDB phục vụ API/dashboard nhanh; HDFS Gold vẫn là analytical source of
truth.

## 9. MapReduce

MapReduce là nhánh validation độc lập cho requirement môn học:

```text
Historical Bronze Reviews
→ mapper/reducer
→ game recommendation metrics
→ compare với Spark game_metrics
```

Cross-check đã được kiểm chứng trên baseline 50 game/25.000 reviews. Dynamic
onboarding workflow hiện không tự chạy lại nhánh MapReduce.

## 10. Machine Learning

Gold Base đã có cấu trúc ML-ready, nhưng MLlib pipeline hoàn chỉnh vẫn là phần
tiếp theo. Experiment cần dùng một snapshot/version cố định để kết quả
reproducible; không nên để streaming âm thầm thay dataset trong một experiment.

## 11. Ví dụ mở rộng 30 game

Với `60 ACTIVE / 106 NEW`, policy chỉ cho 10 game/cycle:

```bash
for cycle in 1 2 3; do
  bash scripts/run_onboarding_workflow.sh --run-discovery || break
done
```

Nếu cả ba cycle PASS:

```text
90 ACTIVE
76 NEW
45.000 historical reviews
```

Review Producer và Player Count Producer đều reload registry ở cycle kế tiếp;
hai Spark Streaming consumer đang chạy không cần restart. Safety limit mặc định
của mỗi producer là 100 ACTIVE games.

## 12. Điều chưa tự động

- Scheduler dùng bounded rotating catalog refresh; không crawl toàn bộ catalog
  Steam trong một cycle.
- WEEKLY scheduler đã có nhưng chưa có OS process supervisor/auto-start.
- Review producer và player-count producer đều reload registry đầu mỗi cycle.
- Chưa có backend API/dashboard cho workflow state.
- Realtime chưa xử lý price/metadata/vote updates. Player-count snapshot đã có
  pipeline riêng.

## Flow ngắn gọn

```text
Steam candidate/probe data
→ Policy
→ Registry
├─ NEW/QUEUED → Historical Workflow → Bronze/Silver/Gold/Mongo → ACTIVE
└─ ACTIVE
   ├─ Review Producer → steam_events → Review Streaming
   └─ Player Count Producer → steam_player_events → Player Count Streaming
      ├─ HDFS incremental/time-series layers
      └─ MongoDB realtime serving
```
