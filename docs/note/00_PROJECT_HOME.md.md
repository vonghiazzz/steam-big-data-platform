# Steam Big Data Platform

## Mục tiêu

Xây dựng nền tảng dữ liệu Steam gồm historical batch, near-real-time review
streaming, analytics và serving:

```text
Steam API
  → Discovery và Game Registry
  → Historical onboarding hoặc realtime polling
  → HDFS Bronze
  → Spark Silver/Gold/Analytics
  → MongoDB
  → API/UI trong tương lai
```

## Trạng thái đã kiểm chứng — 2026-10-01

| Thành phần | Trạng thái |
|---|---|
| Discovery và qualification policy | Đã triển khai |
| Resumable dynamic onboarding | Đã triển khai và chạy thành công |
| HDFS Bronze / Silver / Gold | Đã triển khai |
| Spark Analytics | Đã triển khai |
| MapReduce cross-check | Đã triển khai cho baseline |
| Kafka review producer | Đã triển khai |
| Kafka player-count producer | Đã triển khai |
| Spark Structured Streaming | Đã triển khai cho review và player count |
| MongoDB historical/realtime | Đã triển khai |
| MLlib | Chưa hoàn tất |
| Backend API / dashboard | Chưa triển khai trong repository này |
| Weekly scheduler | Chưa triển khai |
| Producer hot-reload registry | Player count: có; review: chưa, cần restart sau onboarding |

## Snapshot hiện tại

- Registry: `80 ACTIVE`, `87 NEW`, `0 QUEUED`.
- Historical Bronze/Silver/Gold: `80 games`, `40.000 reviews`.
- Unique `recommendationid`: `40.000`.
- Positive: `31.398`.
- Negative: `8.602`.
- MongoDB `game_metrics`: `80` documents.

Baseline nghiên cứu ban đầu vẫn là snapshot cố định `50 games / 25.000
reviews`. Ba mươi game mới đến từ các Dynamic Onboarding batch. Khi viết báo
cáo phải nói rõ đang dùng baseline v1 hay snapshot động hiện tại.

## Tài liệu trong thư mục này

- [Architecture](01_ARCHITECTURE.md.md)
- [Data pipelines](02_DATA_PIPELINES.md.md)
- [Operations runbook](<Code workflow.md>)
- [Streaming explanation](note.md)
- [Steam flow walkthrough](Steam.md)
- [Big Data theory](<Lý thuyết.md>)
- [HDFS/Spark UI](UI.md)

## Nguồn sự thật

Ưu tiên theo thứ tự:

```text
Code và runtime evidence
→ tests
→ config/discovery_policy.json
→ README.md
→ docs/note
```

Không dùng số liệu trong note nếu nó khác kết quả runtime hoặc validation mới
nhất.
