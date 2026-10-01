# Near-real-time, Streaming và Batch trong Steam Big Data Platform

## 1. Kết luận ngắn

Các pipeline review và player count hiện tại là **near-real-time**, không phải
hard real-time.

Nguyên nhân nằm ở nguồn dữ liệu: trong phạm vi API mà project sử dụng, Steam
không chủ động đẩy review mới qua webhook hoặc một event stream. Hệ thống phải
định kỳ gọi Steam Reviews API để kiểm tra. Khi phát hiện review mới, các bước
sau đó mới vận hành theo mô hình event streaming:

```text
Steam Reviews API
        |
        | periodic polling
        v
Review Producer
        |
        | REVIEW_CREATED
        v
Kafka topic: steam_events
        |
        | Spark micro-batch
        v
Bronze -> Silver -> Gold -> MongoDB
```

Vì vậy, cách mô tả chính xác cho hệ thống là:

> A near-real-time Steam review pipeline using periodic API polling, Kafka
> event ingestion, and Spark Structured Streaming micro-batches.

## 2. Phân biệt các khái niệm

### 2.1 Real-time thật

Nguồn phát sự kiện ngay khi thay đổi xảy ra. Ví dụ:

```text
Review được tạo
    -> Steam gửi webhook/event ngay lập tức
    -> Kafka nhận sự kiện
    -> hệ thống xử lý trong vài giây
```

Mô hình này cần nguồn dữ liệu cung cấp webhook, change-data-capture hoặc
persistent event stream. Project hiện không có đầu vào như vậy từ Steam.

### 2.2 Near-real-time

Hệ thống kiểm tra nguồn theo chu kỳ ngắn. Khi phát hiện thay đổi, dữ liệu được
xử lý incrementally thay vì chạy lại toàn bộ lịch sử.

Đây là mô hình hiện tại:

```text
Poll Steam -> phát hiện review mới -> Kafka -> Spark -> HDFS/MongoDB
```

Độ trễ thường được tính bằng phút, chủ yếu phụ thuộc vào chu kỳ polling và
Spark trigger interval.

### 2.3 Batch

Một job có điểm bắt đầu và kết thúc rõ ràng, thường chạy theo lịch dài hơn:

```text
Mỗi giờ hoặc mỗi ngày
    -> lấy một tập dữ liệu
    -> xử lý toàn bộ hoặc xử lý phần tăng thêm
    -> ghi kết quả
    -> job kết thúc
```

Batch phù hợp cho historical backfill, discovery, onboarding, recomputation
và các báo cáo không yêu cầu độ trễ thấp.

## 3. Vì sao không thể lấy review Steam theo real-time thật?

Project không nhận được tín hiệu trực tiếp khi một người dùng vừa tạo review.
Review Producer chỉ biết có dữ liệu mới sau khi gọi API và so sánh kết quả với
state đã biết.

Nếu giảm polling xuống mỗi vài giây thì hệ thống vẫn không trở thành real-time
thật. Nó chỉ trở thành polling dày hơn và tạo ra các vấn đề:

- tăng số request tới Steam;
- tăng nguy cơ rate limit, timeout hoặc tạm chặn;
- lãng phí request khi phần lớn lần kiểm tra không có review mới;
- kéo dài một polling cycle khi số ACTIVE game tăng;
- làm retry và vận hành khó kiểm soát hơn.

Muốn real-time thật cần một nguồn push chính thức hoặc một nhà cung cấp sự kiện
trung gian. Tự scrape liên tục không phải giải pháp an toàn hoặc bền vững.

## 4. Cách Review Producer hiện hoạt động

Producer đọc `game_registry.jsonl` khi khởi động và chỉ lấy các game có trạng
thái `ACTIVE`.

Cấu hình chính trong `.env`:

```env
STREAM_POLL_INTERVAL_SECONDS=600
STREAM_MIN_POLL_INTERVAL_SECONDS=300
STREAM_REQUEST_DELAY_SECONDS=1.0
STREAM_MAX_PAGES_PER_GAME=3
STREAM_MAX_ACTIVE_GAMES=100
STREAM_BOOTSTRAP_SOURCE=hdfs
```

Một vòng xử lý gồm:

1. Đọc danh sách ACTIVE khi producer khởi động.
2. Với từng game, gọi Steam Reviews API.
3. Bootstrap các review ID lịch sử từ HDFS để không phát lại historical data.
4. So sánh review nhận được với SQLite producer state.
5. Chỉ phát review chưa từng biết thành event `REVIEW_CREATED`.
6. Ghi event vào Kafka topic `steam_events`.
7. Sau khi xử lý hết danh sách game, ngủ `600` giây rồi bắt đầu vòng mới.

Điểm cần lưu ý: `600` giây là thời gian ngủ **sau cả vòng**, không phải lịch
poll chính xác 10 phút theo đồng hồ.

```text
Khoảng cách giữa hai lần kiểm tra cùng một game
    = thời gian chạy hết polling cycle
    + STREAM_POLL_INTERVAL_SECONDS
```

Ví dụ với 80 ACTIVE games và khoảng nghỉ tối thiểu 1 giây giữa các game, riêng
phần delay đã gần 79 giây. Thời gian gọi API, phân trang và retry làm cycle dài
hơn nữa. Vì vậy một game thường được kiểm tra lại sau hơn 11 phút, không phải
đúng 10 phút.

Review Producer nạp danh sách ACTIVE một lần lúc khởi động. Sau khi onboarding
thêm game mới, cần restart review producer để game mới đi vào polling scope.
Player Count Producer reload registry ở mỗi cycle và tự nhận ACTIVE game mới.

## 5. Spark Structured Streaming hiện hoạt động thế nào?

Spark chạy liên tục khi:

```env
STREAM_AVAILABLE_NOW=false
```

Nó subscribe Kafka topic `steam_events` và xử lý theo micro-batch. Cấu hình mặc
định:

```env
STREAM_SPARK_TRIGGER_INTERVAL=1 minute
STREAM_MAX_OFFSETS_PER_TRIGGER=1000
STREAM_WATERMARK_DELAY=7 days
```

Review Streaming V1 chỉ hỗ trợ:

```text
REVIEW_CREATED
```

Nó chưa stream các thay đổi về giá, metadata hoặc vote update.
Các trường `voted_up`, `votes_up`, `votes_funny` và `weighted_vote_score` trong
một `REVIEW_CREATED` chỉ là snapshot tại thời điểm phát hiện review mới. V1
không theo dõi lại review cũ để cập nhật các trường này.

Các bước xử lý gồm:

1. Parse Kafka envelope và payload.
2. Kiểm tra `event_type`, schema, game và các trường bắt buộc.
3. Lưu raw event vào Bronze archive.
4. Đưa event không hợp lệ vào quarantine.
5. Dùng watermark và `recommendationid` để deduplicate.
6. Loại review đã tồn tại trong historical Silver baseline.
7. Chuẩn hóa review vào incremental Silver.
8. Join với Silver Games và tạo incremental Gold.
9. Nếu MongoDB realtime được bật, cập nhật serving collections.

Các path chính:

```text
/steam/bronze/stream_events
/steam/quarantine/review_events
/steam/silver/reviews_incremental_v1
/steam/gold/base_incremental_v1
/steam/checkpoints/...
```

MongoDB realtime serving:

```text
recent_reviews
realtime_game_metrics
```

Checkpoint giúp Spark tiếp tục từ offset đã commit sau khi restart. Kafka giữ
sự kiện trong retention window, nên Spark tạm dừng không đồng nghĩa dữ liệu lập
tức bị mất. Tuy nhiên cần khởi động lại trước khi retention hết hạn.

Player Count Streaming V1 là pipeline riêng:

```text
Steam GetNumberOfCurrentPlayers API
-> Player Count Producer
-> Kafka steam_player_events
-> /steam/bronze/player_count_events
-> /steam/silver/player_count_snapshots_v1
-> /steam/gold/player_count_snapshots_v1
-> MongoDB player_count_latest
```

Nó dùng topic, producer SQLite state, HDFS datasets và checkpoints riêng. Mỗi
poll thành công là một observation append-only; MongoDB chỉ upsert latest value
theo `appid`. Cấu hình mặc định poll mỗi 600 giây, giới hạn tối thiểu 300 giây,
và Spark micro-batch mỗi một phút.

## 6. Độ trễ thực tế đến từ đâu?

Có thể biểu diễn gần đúng:

```text
Total latency
    = thời gian chờ producer đến lượt poll game
    + thời gian Steam API/request/retry
    + thời gian Kafka chờ Spark trigger
    + thời gian Spark xử lý và ghi sink
```

Với cấu hình hiện tại:

- producer ngủ 600 giây sau mỗi polling cycle;
- mỗi game có khoảng nghỉ request khoảng 1 giây;
- Spark trigger khoảng 1 phút;
- MongoDB cập nhật sau khi micro-batch tương ứng hoàn tất.

Do đó không nên cam kết con số cố định như “mọi review được cập nhật trong đúng
10 phút”. Cách nói đúng hơn là độ trễ ở mức vài phút đến hơn mười phút, tùy vị
trí game trong cycle, số trang API, retry, tải Kafka/Spark và thời gian ghi HDFS.

## 7. Tại sao vẫn dùng Kafka và Spark Streaming?

Polling chỉ mô tả cách **thu thập từ nguồn**. Streaming mô tả cách hệ thống
**truyền và xử lý thay đổi sau khi phát hiện**. Hai khái niệm này không mâu
thuẫn.

Kafka mang lại:

- tách producer khỏi Spark consumer;
- lưu event bền vững trong retention window;
- cho phép Spark tạm dừng rồi xử lý backlog;
- hỗ trợ thêm consumer khác mà không sửa producer;
- quản lý partition, offset và consumer progress;
- hấp thụ burst khi nhiều review mới xuất hiện cùng lúc.

Spark Structured Streaming mang lại:

- xử lý incremental thay vì đọc lại toàn bộ historical dataset;
- validation và quarantine theo event;
- watermark và deduplication;
- checkpoint và fault recovery;
- cập nhật HDFS và MongoDB theo micro-batch;
- cùng hệ sinh thái với historical Spark batch.

Streaming vẫn có giá trị dù nguồn đầu tiên được polling, đặc biệt khi số game,
số event hoặc số downstream consumer tăng.

## 8. Khi nào batch sẽ hợp lý hơn?

Batch hoặc scheduled incremental batch hợp lý hơn nếu:

- dashboard chỉ cần cập nhật mỗi giờ hoặc mỗi ngày;
- số game và số review mới còn nhỏ;
- không cần Kafka replay hoặc nhiều consumer;
- ưu tiên đơn giản hóa vận hành;
- không muốn duy trì Kafka và Spark chạy liên tục;
- chi phí vận hành quan trọng hơn độ trễ.

Một thiết kế batch đơn giản có thể là:

```text
Scheduler mỗi 10 phút hoặc mỗi giờ
    -> poll Steam
    -> lấy review mới
    -> chạy incremental Spark batch
    -> cập nhật HDFS và MongoDB
    -> kết thúc job
```

Nếu dùng batch, vẫn nên xử lý incrementally dựa trên `recommendationid`. Không
nên đọc và tính lại toàn bộ historical reviews ở mỗi lần chạy.

## 9. So sánh trực tiếp

| Tiêu chí | Scheduled incremental batch | Kafka + Spark Streaming hiện tại |
|---|---|---|
| Độ phức tạp | Thấp hơn | Cao hơn |
| Tiến trình chạy liên tục | Không | Có |
| Độ trễ | Theo lịch job | Theo polling + micro-batch |
| Replay khi consumer dừng | Phải tự quản lý | Kafka + checkpoint hỗ trợ |
| Nhiều downstream consumer | Khó mở rộng hơn | Tự nhiên hơn |
| Dedup/state | Tự cài trong từng job | Có state/checkpoint trong pipeline |
| Phù hợp dữ liệu lịch sử | Rất phù hợp | Không cần thiết |
| Phù hợp review mới | Đủ nếu SLA dài | Phù hợp nếu cần cập nhật theo phút |
| Giá trị học thuật BDA501 | Batch processing | Event streaming và fault recovery |

## 10. Lựa chọn phù hợp cho project

Thiết kế phù hợp nhất là **hybrid architecture**:

| Nhu cầu | Cơ chế |
|---|---|
| Catalog discovery | Periodic batch |
| Qualification policy | Batch/control plane |
| Dynamic onboarding | Bounded workflow |
| 500 historical reviews cho game mới | Historical batch backfill |
| Bronze -> Silver -> Gold lịch sử | Spark batch |
| Phát hiện review mới | Periodic Steam API polling |
| Truyền review mới | Kafka event stream |
| Xử lý review mới | Spark Structured Streaming |
| Concurrent player count | Polling + Kafka + Spark Structured Streaming |
| Dashboard/serving gần thời gian thực | MongoDB |

Batch và streaming không thay thế hoàn toàn cho nhau:

- Batch chịu trách nhiệm completeness và reproducibility của lịch sử.
- Streaming chịu trách nhiệm freshness của thay đổi mới.
- Registry và onboarding policy quyết định game nào được cả hai luồng quản lý.

## 11. Những gì “realtime” hiện đang cập nhật

Review Producer chỉ phát review mới chưa từng thấy. Spark Streaming cập nhật:

- raw `REVIEW_CREATED` event trong HDFS Bronze;
- review hợp lệ, typed và deduplicated trong incremental Silver;
- review đã join game metadata trong incremental Gold;
- recent review serving data trong MongoDB;
- realtime game-level metrics trong MongoDB.

Cụ thể, một review mới có thể mang theo `voted_up`, vote counters, weighted
score, playtime và purchase/free flags. Những giá trị này đi từ payload mới qua
Silver/Gold và MongoDB. “Realtime metrics” ở đây có nghĩa là aggregate trên
review incremental đã nhận; nó không có nghĩa là concurrent player count hoặc
toàn bộ historical metric được recompute mỗi phút.

Review Streaming hiện không tự động:

- discovery game mới;
- chuyển game từ NEW/QUEUED sang ACTIVE;
- cập nhật metadata hoặc giá game;
- cập nhật vote của review cũ;
- reload danh sách ACTIVE trong producer đang chạy.

Concurrent player count đã có luồng riêng và player-count producer reload ACTIVE
registry mỗi cycle. Các game vừa onboarding chỉ cần restart **review producer**;
player-count producer tự nhận ở cycle kế tiếp.

### 11.1 Trạng thái mở rộng streaming

Có thể đưa price, metadata, review vote updates và player count vào cùng nền
tảng, nhưng không nên dùng chung một payload, state model hoặc polling cadence.

| Event | Semantics | Cadence gợi ý | Silver dataset đề xuất |
|---|---|---|---|
| `REVIEW_CREATED` | Review mới | khoảng 10 phút | `reviews_incremental_v1` |
| `REVIEW_UPDATED` | Mutable review fields đổi | 1-6 giờ, recent window | `review_updates_v1` |
| `GAME_PRICE_CHANGED` | Price/currency/discount đổi | 1-6 giờ | `game_price_changes_v1` |
| `GAME_METADATA_CHANGED` | Selected metadata đổi | 6-24 giờ | `game_metadata_changes_v1` |
| `PLAYER_COUNT_SNAPSHOT` | Time-series observation; **đã triển khai** | mặc định 10 phút | `player_count_snapshots_v1` |

Price và metadata có thể được thu bởi một Game State Producer nhưng phải tạo
contract riêng. Player count luôn append theo observation time. Review update
phải dùng bounded recent-review window; không poll lại toàn bộ historical
reviews trong mỗi cycle.

Player count đã chọn topic domain riêng `steam_player_events`. Với price,
metadata hoặc review-update, publish thẳng event mới vào `steam_events` vẫn
không an toàn vì review parser V1 chỉ chấp nhận `REVIEW_CREATED`. Hai lựa chọn:

1. refactor consumer thành dispatcher theo `event_type`; hoặc
2. dùng các topic domain riêng cho review, game state và player snapshots.

Khuyến nghị là dùng cùng Kafka cluster nhưng topic/checkpoint/Silver sink riêng
cho từng domain. Gold và MongoDB mới là nơi hợp nhất current state và analytics.

Thứ tự triển khai an toàn:

1. `PLAYER_COUNT_SNAPSHOT`;
2. `GAME_PRICE_CHANGED` và `GAME_METADATA_CHANGED`;
3. `REVIEW_UPDATED`;
4. dashboard/alerting trên các Gold datasets mới.

Mục 1 đã triển khai trong Player Count V1. Các mục 2-4 vẫn là kế hoạch, chưa
phải chức năng runtime hiện có.

## 12. Cách vận hành đúng

Bốn tiến trình phải chạy riêng nếu muốn bật đồng thời cả hai domain:

```text
Terminal 1: Review Spark Structured Streaming
Terminal 2: Review Producer
Terminal 3: Player Count Spark Structured Streaming
Terminal 4: Player Count Producer
```

Review Spark chạy liên tục với `STREAM_AVAILABLE_NOW=false`; player-count Spark
chạy liên tục với `PLAYER_COUNT_AVAILABLE_NOW=false`. Hai producer cũng phải
chạy liên tục nếu muốn polling lặp lại; tùy chọn `--once` chỉ chạy một cycle rồi
kết thúc.

Nếu Spark dừng nhưng producer còn chạy:

- producer vẫn có thể ghi review mới vào Kafka;
- Spark sẽ đọc backlog khi chạy lại;
- cần để ý Kafka retention và lỗi checkpoint.

Nếu producer dừng nhưng Spark còn chạy:

- Spark vẫn hoạt động và chờ Kafka;
- không có event mới nếu không có producer khác;
- terminal Spark đứng yên không có nghĩa là bị treo.

## 13. Hướng cải tiến trong tương lai

### 13.1 Scheduler và service supervision

Dùng cron, launchd, systemd, Docker Compose healthcheck hoặc một orchestrator để
khởi động lại producer/streaming khi process lỗi.

### 13.2 Dynamic ACTIVE-game reload

Player-count producer đã reload registry sau mỗi cycle. Review producer vẫn cần
được bổ sung cơ chế tương tự để game vừa onboarding được theo dõi mà không phải
restart process.

### 13.3 Adaptive polling

Game có review volume cao được poll thường xuyên hơn; game ít hoạt động được
poll thưa hơn. Cách này giảm request nhưng vẫn giữ freshness cho game phổ biến.

### 13.4 Stable HDFS endpoint

Dùng một hostname ổn định cho HDFS. Spark checkpoint và `_spark_metadata` có thể
lưu URI tuyệt đối; thay đổi NameNode authority giữa các lần chạy có thể làm
query không đọc lại được metadata cũ.

### 13.5 Metrics và alerting

Theo dõi:

- producer cycle duration;
- request error/rate-limit count;
- Kafka consumer lag;
- Spark micro-batch duration;
- event validation/quarantine count;
- thời gian từ `timestamp_created` đến MongoDB serving;
- lần thành công cuối của producer và Spark.

### 13.6 Các event domain tiếp theo

Player count đã dùng versioned envelope và topic/state/checkpoint/sink riêng.
Price, metadata và review-update phải tiếp tục theo cùng nguyên tắc: không dùng
một global polling interval cho mọi API; mỗi producer phải có request budget,
retry policy, idempotency key và checkpoint/sink riêng trước khi bật runtime.

## 14. Câu trả lời ngắn khi trình bày

**Tại sao gọi là streaming nếu Steam được polling?**

> Steam là pull-based source nên producer phải polling. Sau khi phát hiện review
> mới, mỗi review được biểu diễn thành một event độc lập, lưu trong Kafka và xử
> lý incrementally bằng Spark Structured Streaming. Vì vậy toàn hệ thống là
> near-real-time, không phải hard real-time.

**Tại sao không dùng batch hoàn toàn?**

> Batch phù hợp với historical backfill và onboarding. Streaming tránh việc đọc
> lại toàn bộ lịch sử khi chỉ có vài review mới, đồng thời cung cấp Kafka replay,
> checkpoint, deduplication và cập nhật MongoDB theo micro-batch.

**Nếu chỉ có 80-100 game thì Kafka có bắt buộc không?**

> Không bắt buộc về mặt tải dữ liệu. Scheduled incremental batch có thể đủ cho
> quy mô hiện tại. Kafka và Spark Streaming được giữ để đáp ứng mục tiêu kiến
> trúc event-driven, fault recovery, mở rộng downstream consumer và yêu cầu học
> thuật của nền tảng Big Data.

**Hệ thống có cập nhật ngay khi Steam có review không?**

> Không. Review chỉ được phát hiện ở polling cycle kế tiếp, sau đó Kafka và Spark
> xử lý trong micro-batch. Độ trễ thực tế thường ở mức phút.
