# Steam Near-real-time Streaming

## Trạng thái hiện tại

Kafka và Spark Structured Streaming đã được triển khai cho hai luồng độc lập và
đã qua live end-to-end validation:

```text
steam_events        -> REVIEW_CREATED
steam_player_events -> PLAYER_COUNT_SNAPSHOT
```

Review V1 chỉ phát review mới chưa từng thấy. Player Count V1 lấy concurrent
player count theo chu kỳ và phát một time-series snapshot cho mỗi ACTIVE game.
Hệ thống chưa quay lại quan sát review cũ để phát vote update và chưa poll price
hoặc game metadata. Các trường vote trong review payload chỉ là snapshot tại
thời điểm review mới được phát hiện.

## Player Count V1 đang cập nhật gì?

```text
ACTIVE Game Registry (reload mỗi cycle)
-> Steam GetNumberOfCurrentPlayers API
-> PLAYER_COUNT_SNAPSHOT
-> Kafka topic steam_player_events, key = appid
-> HDFS Bronze/Silver/Gold time series
-> MongoDB player_count_latest
```

Mỗi lần poll thành công đều tạo một snapshot append-only, kể cả khi số người
chơi không đổi. SQLite outbox bảo đảm event chưa publish thành công sẽ được thử
lại. MongoDB giữ một document mới nhất cho mỗi `appid`; HDFS giữ lịch sử.

Các path riêng của player count:

```text
/steam/bronze/player_count_events
/steam/quarantine/player_count_events
/steam/silver/player_count_snapshots_v1
/steam/gold/player_count_snapshots_v1
/steam/checkpoints/player_count_*_v1
```

## V1 đang cập nhật chính xác những gì?

Khi producer phát hiện một `recommendationid` mới, nó phát review payload hiện
quan sát được. Các trường được dùng trong pipeline gồm:

- `voted_up`;
- `votes_up`, `votes_funny` và `weighted_vote_score`;
- `playtime_at_review` và `playtime_forever`;
- `steam_purchase` và `received_for_free`;
- `timestamp_created`.

Spark xử lý event đó và cập nhật:

```text
HDFS Bronze: raw REVIEW_CREATED event
HDFS Silver: typed + validated + deduplicated incremental review
HDFS Gold: review joined với game metadata
MongoDB recent_reviews: recent review serving documents
MongoDB realtime_game_metrics: aggregate từ incremental reviews
```

`realtime_game_metrics` là metric của luồng review incremental. Nó không thay
thế historical `game_metrics` và không phải concurrent player count. Nếu một
review cũ nhận thêm helpful/funny votes, V1 không phát event mới cho thay đổi
đó.

## Kafka nhận dữ liệu từ Steam như thế nào?

Kafka không gọi Steam API. Review Producer chủ động poll Steam rồi publish event:

```text
ACTIVE Game Registry
→ Review Producer
→ Steam appreviews API
→ phát hiện recommendationid chưa từng thấy
→ REVIEW_CREATED event
→ Kafka topic steam_events, key = appid
```

Producer dùng SQLite state để lưu bootstrap, known review IDs, high-water state
và outbox. Nếu Steam không có review mới, producer không emit event.

## Các khoảng thời gian

- Producer ngủ 600 giây sau khi hoàn tất một ACTIVE polling cycle.
- Delay giữa hai request: khoảng một giây.
- Spark Structured Streaming: micro-batch khoảng một phút.
- MongoDB cập nhật sau khi micro-batch commit thành công.

Một giây không có nghĩa là mỗi game được poll mỗi giây. Khoảng cách giữa hai lần
poll cùng một game bằng thời gian xử lý cả ACTIVE scope cộng thêm 600 giây.

## Event contract

Event chứa envelope versioned và raw review payload, ví dụ rút gọn:

```json
{
  "event_id": "review-created-440-123456789",
  "event_type": "REVIEW_CREATED",
  "schema_version": 1,
  "appid": 440,
  "recommendationid": "123456789",
  "event_time": "2026-10-01T02:00:00Z",
  "produced_at": "2026-10-01T02:00:04Z",
  "payload": {
    "voted_up": true,
    "review": "Example review"
  }
}
```

Schema đầy đủ phải lấy từ `src/streaming/event_contract.py`. Review consumer chỉ
chấp nhận `REVIEW_CREATED`; player-count consumer riêng chỉ chấp nhận
`PLAYER_COUNT_SNAPSHOT`. Event sai topic/contract sẽ bị quarantine.

## Kafka và Structured Streaming

```text
Kafka steam_events
→ parse + schema validation
→ invalid event quarantine
→ event-time/watermark
→ deduplicate event_id
├─ immutable HDFS Bronze archive
├─ incremental Silver reviews
├─ incremental Gold rows
├─ MongoDB recent_reviews
└─ MongoDB realtime_game_metrics
```

Các path chính:

```text
/steam/bronze/stream_events
/steam/quarantine/review_events
/steam/silver/reviews_incremental_v1
/steam/gold/base_incremental_v1
/steam/checkpoints/*
```

Kafka là transport/distributed log. HDFS là nguồn raw lâu dài để audit và
replay. MongoDB là serving view.

## Game mới sau onboarding

Workflow chỉ chuyển game sang `ACTIVE` sau khi historical Bronze, Silver, Gold
và MongoDB readiness đều PASS. Producer bootstrap 500 historical review IDs từ
HDFS, do đó không phát lại backfill vào Kafka.

Review producer và player-count producer reload ACTIVE scope ở đầu mỗi cycle.
Game vừa được activate sẽ được bootstrap state và tự tham gia polling mà không
cần restart. Hai Spark Streaming consumer cũng không cần restart.

## Scheduling

`config/discovery_policy.json` ghi discovery frequency là `WEEKLY` và
`scripts/run_discovery_scheduler.sh` thực thi lịch bằng state theo UTC ISO week.
Scheduler và hai streaming producer chỉ tự loop khi process đang chạy; phần còn
thiếu là process supervisor/OS auto-start.

## Có nên bổ sung price, metadata, vote và player count cùng lúc?

Có thể bổ sung trong cùng Steam Big Data Platform và cùng Kafka cluster, nhưng
không nên nhét tất cả vào một `REVIEW_CREATED` payload hoặc dùng chung một
polling interval. Các loại thay đổi có semantics và tần suất khác nhau:

| Event đề xuất | Ý nghĩa | Polling đề xuất | State/dedup key |
|---|---|---|---|
| `REVIEW_CREATED` | Review mới | khoảng 10 phút | `recommendationid` |
| `REVIEW_UPDATED` | Mutable fields của review cũ đổi | 1-6 giờ, recent window | `recommendationid + content_hash` |
| `GAME_PRICE_CHANGED` | Giá/currency/discount đổi | 1-6 giờ | `appid + price_hash` |
| `GAME_METADATA_CHANGED` | Name, genres, categories, platforms... đổi | 6-24 giờ | `appid + metadata_hash` |
| `PLAYER_COUNT_SNAPSHOT` | Concurrent players tại một thời điểm; **đã triển khai V1** | mặc định 10 phút | `appid + observation time` |

Price và metadata có thể dùng chung một Game State Producer vì cùng bắt đầu từ
game-details snapshot, nhưng nên phát hai contract khác nhau. Player count là
time-series snapshot, không phải change event. Vote update chỉ nên kiểm tra một
recent/bounded review window; poll lại toàn bộ historical reviews mỗi cycle sẽ
tốn request và không mở rộng được.

### Topic strategy

Topic strategy hiện tại và phần mở rộng đề xuất:

```text
steam_review_events : REVIEW_CREATED, REVIEW_UPDATED
steam_game_events   : GAME_PRICE_CHANGED, GAME_METADATA_CHANGED
steam_player_events : PLAYER_COUNT_SNAPSHOT (đã triển khai)
```

Các topic dùng cùng Kafka cluster và key bằng `appid`, nhưng có schema,
checkpoint và Silver sink riêng. Ở quy mô bài tập có thể dùng một topic
`steam_events`, nhưng chỉ sau khi consumer được refactor để route theo
`event_type`. Consumer V1 hiện tại không hỗ trợ việc đó.

### Thứ tự triển khai V2

1. Version common envelope và thêm unit tests cho từng event contract.
2. Thêm state store/hash comparison cho game snapshot và review update.
3. Tạo producer riêng theo cadence, retry và request budget riêng.
4. Tạo Bronze archive và quarantine cho từng event family.
5. Tạo Silver datasets riêng; không trộn player snapshots với review rows.
6. Tạo Gold current-state/history/time-series tables.
7. Cập nhật MongoDB idempotently và thêm runtime validation.
8. Chỉ bật producer khi consumer, checkpoint và sink tương ứng đã sẵn sàng.

Player count đã hoàn thành thành một slice riêng. Slice tiếp theo nên là
price/metadata, rồi mới review vote update vì vote update cần chính sách recent
window và state lớn hơn.
