# Architecture

## Luồng end-to-end hiện tại

```text
                         Steam API
                            │
               ┌────────────┴────────────┐
               │                         │
       Discovery/onboarding       ACTIVE review polling
               │                         │
       Policy + Registry                Kafka
               │                         │
       Historical backfill       Structured Streaming
               │                  │       │       │
               ▼                  │       │       └─ quarantine
          HDFS Bronze ◀───────────┘       ├─ incremental Silver/Gold
               │                          └─ MongoDB realtime
               ▼
          Spark Silver
               ▼
           Gold Base
               ▼
        Gold Analytics
          │          │
          │          └─ MongoDB historical serving
          └─ visualization / SQL
```

## Dynamic onboarding

Một batch onboarding chạy theo state machine:

```text
Runtime
→ Discovery
→ Prepare
→ Crawl
→ Validate
→ Publish Bronze
→ Verify Bronze
→ Silver
→ Gold
→ Analytics
→ MongoDB
→ Readiness
→ QUEUED → ACTIVE
```

Workflow được lock để không chạy hai batch đồng thời và ghi trạng thái tại:

```text
data/onboarding/<batch-id>/workflow_state.json
```

Chạy lại cùng `batch-id` sẽ skip các phase đã `COMPLETED`. Bronze là immutable:
file đã tồn tại chỉ được tái sử dụng nếu nội dung giống staging; workflow không
overwrite một object Bronze khác nội dung.

## Trách nhiệm lưu trữ

| Thành phần | Trách nhiệm |
|---|---|
| Game Registry | Lifecycle `NEW/QUEUED/ACTIVE/...` và phạm vi polling |
| Kafka | Event transport và distributed log |
| HDFS Bronze | Raw, immutable, replayable |
| HDFS Silver | Clean, typed, validated, deduplicated |
| HDFS Gold Base | Một dòng cho một review, analytics/ML-ready |
| Gold Analytics | Các bảng aggregate phục vụ phân tích |
| MongoDB | Serving/materialized view; không phải analytical source of truth |
| SQLite producer state | Review IDs đã biết, bootstrap và outbox producer |
| Streaming checkpoints | Kafka offsets và state của Spark Streaming |

## Ranh giới current và future

Review realtime hỗ trợ `REVIEW_CREATED`; Player Count V1 hỗ trợ
`PLAYER_COUNT_SNAPSHOT` trên topic/checkpoint riêng. Price update, metadata
update và vote update chưa được triển khai.

Discovery frequency `WEEKLY` được thực thi bởi lightweight scheduler có
rotating catalog cursor, ACTIVE-capacity guard, lock và resumable batch ID.
Review Producer và Player Count Producer reload danh sách `ACTIVE` mỗi cycle,
nên game mới tự vào polling scope mà không cần restart. Backend API/UI sau này
nên trigger workflow và đọc `workflow_state.json`, không sửa registry/HDFS trực
tiếp.
