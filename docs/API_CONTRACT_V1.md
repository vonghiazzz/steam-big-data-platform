1. Response envelope chung
Object:
{
  "data": {
    "..."
  }
}

List:
{
  "data": [
    {}
  ],
  "meta": {
    "count": 10
  }
}

List có pagination:
{
  "data": [
    {}
  ],
  "meta": {
    "limit": 20,
    "next_cursor": "236525130",
    "has_more": true
  }
}

Không cần nhét:
{
  "success": true,
  "status": 200,
  "message": "OK"
}

vào mọi response. HTTP status đã thể hiện việc đó.
2. Health
GET /api/health

{
  "data": {
    "status": "UP",
    "mongodb": "UP"
  }
}

Đủ.
3. Historical Analytics
Top games
GET /api/analytics/games/top?limit=10

{
  "data": [
    {
      "appid": 413150,
      "game_name": "Stardew Valley",
      "review_count": 500,
      "positive_reviews": 496,
      "negative_reviews": 4,
      "recommendation_rate": 0.992
    }
  ],
  "meta": {
    "count": 10,
    "limit": 10
  }
}

Type chốt:
appid                 integer
game_name             string
review_count           integer
positive_reviews       integer
negative_reviews       integer
recommendation_rate    number 0..1

Genres
GET /api/analytics/genres

{
  "data": [
    {
      "genre": "Action",
      "review_count": 12500,
      "positive_reviews": 9100,
      "negative_reviews": 3400,
      "recommendation_rate": 0.728
    }
  ],
  "meta": {
    "count": 17
  }
}

UNKNOWN là value hợp lệ, frontend không được coi là lỗi.
Playtime
GET /api/analytics/playtime

{
  "data": [
    {
      "playtime_bucket": "0-2h",
      "review_count": 917,
      "positive_reviews": 293,
      "negative_reviews": 624,
      "recommendation_rate": 0.31952,
      "avg_playtime_hours": 0.895
    }
  ],
  "meta": {
    "count": 5
  }
}

Bucket V1:
0-2h
2-10h
10-50h
50h+
MISSING

Không để frontend tự suy ra bucket.
Free vs Paid
GET /api/analytics/free-paid

{
  "data": [
    {
      "game_type": "FREE",
      "game_count": 13,
      "review_count": 6500,
      "positive_reviews": 4522,
      "negative_reviews": 1978,
      "recommendation_rate": 0.695692
    },
    {
      "game_type": "PAID",
      "game_count": 37,
      "review_count": 18500,
      "positive_reviews": 13799,
      "negative_reviews": 4701,
      "recommendation_rate": 0.745892
    }
  ],
  "meta": {
    "count": 2
  }
}

Nếu Mongo hiện có thêm price fields thì Backend có thể trả chúng, nhưng cần lấy đúng tên field thực tế từ collection trước khi khóa contract.
Platform
GET /api/analytics/platforms

{
  "data": [
    {
      "platform": "WINDOWS",
      "review_count": 21000,
      "positive_reviews": 15500,
      "negative_reviews": 5500,
      "recommendation_rate": 0.738095
    }
  ],
  "meta": {
    "count": 3
  }
}

Nhớ ghi chú:
Platform là multi-membership, tổng review_count giữa các platform có thể lớn hơn 25.000.

Frontend không được cộng lại rồi báo lỗi.
Category
GET /api/analytics/categories

{
  "data": [
    {
      "category": "Single-player",
      "review_count": 15000,
      "positive_reviews": 11200,
      "negative_reviews": 3800,
      "recommendation_rate": 0.746667
    }
  ],
  "meta": {
    "count": 59
  }
}

Category cũng multi-membership.
Purchase source
GET /api/analytics/purchase

{
  "data": [
    {
      "purchase_source": "STEAM_PURCHASE",
      "review_count": 21094,
      "positive_reviews": 15714,
      "negative_reviews": 5380,
      "recommendation_rate": 0.744951
    },
    {
      "purchase_source": "OTHER_SOURCE",
      "review_count": 3906,
      "positive_reviews": 2607,
      "negative_reviews": 1299,
      "recommendation_rate": 0.667435
    }
  ],
  "meta": {
    "count": 2
  }
}

4. Realtime reviews
Đây là contract Bình cần nhất.
GET /api/realtime/reviews?limit=20

hoặc:
GET /api/realtime/reviews?appid=570&limit=20

Response:
{
  "data": [
    {
      "recommendationid": "236525130",
      "appid": 570,
      "voted_up": true,
      "playtime_at_review": 428,
      "playtime_forever": 1500,
      "steam_purchase": true,
      "received_for_free": false,
      "timestamp_created": "2026-09-30T12:49:46Z",
      "stream_ingested_at": "2026-09-30T12:50:03Z"
    }
  ],
  "meta": {
    "limit": 20,
    "next_cursor": "236525130",
    "has_more": true
  }
}

Chốt type:
recommendationid       string
appid                  integer
voted_up               boolean
playtime_at_review     integer | null
playtime_forever       integer | null
steam_purchase         boolean | null
received_for_free      boolean | null
timestamp_created      ISO-8601 UTC string
stream_ingested_at     ISO-8601 UTC string | null

Mình khuyên identifier như recommendationid luôn để string.
5. Realtime game metrics
Tất cả game/window gần nhất
GET /api/realtime/games?limit=20

Response:
{
  "data": [
    {
      "appid": 570,
      "window_start": "2026-09-30T12:00:00Z",
      "window_end": "2026-09-30T13:00:00Z",
      "review_count": 3,
      "positive_reviews": 3,
      "negative_reviews": 0,
      "recommendation_rate": 1.0
    }
  ],
  "meta": {
    "count": 20,
    "limit": 20
  }
}

Một game:
GET /api/realtime/games/570?limit=24

{
  "data": [
    {
      "appid": 570,
      "window_start": "2026-09-30T12:00:00Z",
      "window_end": "2026-09-30T13:00:00Z",
      "review_count": 3,
      "positive_reviews": 3,
      "negative_reviews": 0,
      "recommendation_rate": 1.0
    }
  ],
  "meta": {
    "count": 1
  }
}

Frontend có thể lấy cái này để vẽ:
time
 │
 │     ╭─╮
 │   ╭─╯ ╰─╮
 │───╯     ╰──
 └─────────────
 recommendation rate

6. Error contract phải chốt ngay
Đừng để mỗi controller trả lỗi một kiểu.
Ví dụ query sai:
GET /api/realtime/reviews?limit=-5

HTTP:
400 Bad Request

JSON:
{
  "error": {
    "code": "INVALID_ARGUMENT",
    "message": "limit must be between 1 and 100"
  }
}

Không tìm thấy:
{
  "error": {
    "code": "NOT_FOUND",
    "message": "Game 999999 was not found"
  }
}

Mongo lỗi:
{
  "error": {
    "code": "SERVICE_UNAVAILABLE",
    "message": "Analytics data is temporarily unavailable"
  }
}

Không trả stacktrace ra frontend.
7. Query parameter cũng phải khóa
Chốt V1 như này là đủ:
Endpoint	Params
/analytics/games/top	limit=1..50
/realtime/reviews	appid?, limit=1..100, cursor?
/realtime/games	appid?, limit=1..100
/realtime/games/{appid}	limit=1..100


Không cần ngay V1:
sortBy
sortDirection
dynamic field filters
complex date expressions
full text search

Scope sẽ phình rất nhanh.
8. Timestamp phải thống nhất
Đây là cái Khánh/Bình dễ lệch nhau nhất.
API luôn trả UTC ISO-8601:
"timestamp_created": "2026-09-30T12:49:46Z"

Không trả:
"timestamp_created": 1790772586

và cũng không trả kiểu Mongo:
{
  "$date": "..."
}

Frontend muốn hiển thị giờ Việt Nam thì convert:
UTC API
   ↓
Browser/frontend
   ↓
Asia/Ho_Chi_Minh

Backend không cần đổi timezone theo máy người dùng.
9. Mongo _id không cần expose
Mongo có:
{
  "_id": "236525130"
}

Nhưng API chỉ cần:
{
  "recommendationid": "236525130"
}

Tương tự realtime window _id:
570:2026-09-30T12:00:00Z:...

là implementation detail.
Không cần Bình phụ thuộc vào _id.


MOCK CHÍNH
top-games.json
{
  "data": [
    {
      "appid": 413150,
      "game_name": "Stardew Valley",
      "review_count": 500,
      "positive_reviews": 496,
      "negative_reviews": 4,
      "recommendation_rate": 0.992
    }
  ],
  "meta": {
    "count": 1
  }
}

recent-reviews.json
{
  "data": [
    {
      "recommendationid": "236525130",
      "appid": 570,
      "voted_up": true,
      "playtime_at_review": 428,
      "playtime_forever": 1500,
      "steam_purchase": true,
      "received_for_free": false,
      "timestamp_created": "2026-09-30T12:49:46Z",
      "stream_ingested_at": "2026-09-30T12:50:03Z"
    }
  ],
  "meta": {
    "limit": 20,
    "next_cursor": null,
    "has_more": false
  }
}

realtime-metrics.json
{
  "data": [
    {
      "appid": 570,
      "window_start": "2026-09-30T12:00:00Z",
      "window_end": "2026-09-30T13:00:00Z",
      "review_count": 3,
      "positive_reviews": 3,
      "negative_reviews": 0,
      "recommendation_rate": 1.0
    }
  ],
  "meta": {
    "count": 1
  }
}

Khánh phải làm API sao cho output tương thích với các mock đó.