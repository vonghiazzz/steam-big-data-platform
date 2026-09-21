# Steam Project Structure & Data Storage Notes

> **Scope note:** This is a detailed storage/cleanup working note. The current architecture source of truth is [`../00_PROJECT_OVERVIEW.md`](../00_PROJECT_OVERVIEW.md), [`../01_ARCHITECTURE.md`](../01_ARCHITECTURE.md), and [`../12_ARCHITECTURE_DECISIONS.md`](../12_ARCHITECTURE_DECISIONS.md). Do not execute cleanup suggestions without a fresh repository audit and explicit approval.

## 1. Nguyên tắc tổng thể

Project nên tách rõ 3 nơi lưu trữ:

```text
Git Repository
→ chứa CODE + DOCS + small evidence

HDFS
→ chứa DATA LAKE thật
→ Bronze / Silver / Gold

Archive ngoài Git repo
→ chứa RAW BACKUP lớn
```

Mental model cần nhớ:

```text
Git
=
CODE

HDFS
=
DATA LAKE

Archive
=
RAW BACKUP
```

Không nên:

```text
Git repo
→ raw data lớn
→ Silver/Gold Parquet lớn
→ hàng nghìn review pages
```

---

# 2. Data Flow cuối cùng

```text
Steam API
    ↓
Python Crawler
    ↓
Local Landing
    ↓
Validation
    ↓
Bronze-ready
    ↓
HDFS Bronze
    ↓
PySpark ETL
    ↓
HDFS Silver
    ↓
PySpark Transformation / Join
    ↓
HDFS Gold
    ↓
├── Spark SQL / EDA
├── Spark MLlib
└── MongoDB Serving
```

Streaming:

```text
ACTIVE games
    ↓
Steam API polling
    ↓
Detect changes / create events
    ↓
Kafka
    ↓
Structured Streaming
    ↓
Validate / Dedup / Watermark / State
    ↓
Silver / Gold incremental update
```

Batch và Streaming cùng dùng chung:

```text
Bronze
→ Silver
→ Gold
```

---

# 3. `data/raw/` dùng để làm gì?

`data/raw/` là dữ liệu local phục vụ quá trình crawl và chuẩn bị trước khi upload lên HDFS.

Nó KHÔNG phải Data Lake chính thức.

Flow:

```text
Steam API
↓
data/raw/
↓
Validation
↓
bronze_ready
↓
HDFS Bronze
```

---

# 4. `data/raw/catalog_probe/`

Mục đích:

```text
Steam Catalog
↓
Discovery
↓
Candidate Games
↓
Qualification
```

Các file kiểu:

```text
candidates.jsonl
eligible_games.jsonl
metadata_probe_raw.jsonl
review_probe.jsonl
page_*.json
```

Dùng để:

- tìm game candidate
- xem metadata game
- kiểm tra review availability
- kiểm tra game có đủ điều kiện crawl
- tạo candidate list

Sau khi đã chọn xong 50 game thì đây chỉ còn là intermediate data.

### Quyết định

```text
Có thể archive
Không cần cho pipeline final hằng ngày
Không push GitHub
```

---

# 5. `data/raw/selected_50_games.jsonl`

Đây là file NÊN GIỮ cho initial research snapshot.

Flow:

```text
Discovery
↓
Qualification
↓
Selection
↓
selected_50_games.jsonl
```

Vai trò:

- danh sách game của fixed/versioned 50-game research cohort
- giúp reproduce crawl
- input cho crawler
- evidence cho selection process
- có thể seed future dynamic Game Registry / Watchlist

File này không phải permanent architecture limit.

### Quyết định

```text
KEEP
```

File nhỏ thì có thể push Git nếu không chứa dữ liệu nhạy cảm.

---

# 6. `data/raw/landing/`

Đây là output chính từ crawler.

Ví dụ:

```text
landing/
├── crawl_state.json
├── games/
│   └── games_raw.jsonl
├── reviews_by_game/
├── review_pages/
└── validation/
```

Flow:

```text
Steam API
↓
Python Crawler
↓
landing/
```

`landing` là staging area.

Nó vẫn còn gần raw source và chưa phải Silver/Gold.

---

# 7. `data/raw/landing/games/`

Chứa game metadata.

Ví dụ:

```text
games_raw.jsonl
```

Các field chính:

```text
appid
name
developers
publishers
genres
categories
is_free
price_overview
platforms
release_date
```

Vai trò:

```text
Game Metadata Source
```

Sau này dùng join với reviews bằng:

```text
games.appid
=
reviews.appid
```

---

# 8. `data/raw/landing/reviews_by_game/`

Chứa review theo từng game.

Ví dụ:

```text
730.jsonl
570.jsonl
4000.jsonl
...
```

Mỗi file đại diện review của một `appid`.

Flow:

```text
appid
↓
Steam Reviews
↓
JSONL
```

Đây là nguồn chính để tạo HDFS Bronze reviews.

---

# 9. `data/raw/landing/review_pages/`

Chứa raw response theo từng API page.

Ví dụ:

```text
review_pages/
└── 730/
    ├── page_0001.json
    ├── page_0002.json
    └── ...
```

Dùng để:

- debug crawler
- kiểm tra pagination
- raw evidence
- replay crawl logic
- investigate lỗi API

Nhược điểm:

```text
rất nhiều file
tốn dung lượng
không cần cho Spark analytics trực tiếp
```

### Quyết định

Sau khi crawl và validation ổn:

```text
MOVE TO ARCHIVE
```

Không nên để lâu trong active Git repo.

---

# 10. `data/raw/landing/validation/`

Chứa validation report.

Ví dụ:

```text
landing_validation_report.json
```

Dùng kiểm tra:

```text
missing game
missing reviews
invalid rows
incomplete crawl
duplicate
schema issue
```

### Quyết định

Report nhỏ có thể:

```text
KEEP
```

hoặc copy sang:

```text
evidence/
```

---

# 11. `data/raw/bronze_ready/`

Đây là dữ liệu đã qua:

```text
crawl
↓
validation
↓
finalization
```

và sẵn sàng upload lên HDFS.

Ví dụ:

```text
bronze_ready/
├── reviews_by_game/
├── finalization_report.json
└── validation_report.json
```

Flow:

```text
landing
↓
validation
↓
bronze_ready
↓
HDFS Bronze
```

Điểm quan trọng:

```text
bronze_ready
≠
HDFS Bronze
```

`bronze_ready` chỉ có nghĩa:

```text
Ready To Become Bronze
```

Bronze chính thức nằm trong HDFS.

### Quyết định

Hiện tại:

```text
KEEP TẠM
```

Sau khi:

```text
HDFS Bronze verified
+
row count verified
+
raw backup đã có
```

thì có thể move ra archive.

---

# 12. `data/raw/landing_filter_all_archive/`

Đây là archive crawl cũ.

Nó chứa rất nhiều:

```text
review_pages/
reviews_by_game/
games/
validation/
```

Một số game có hàng nghìn raw page JSON.

Folder này không thuộc active pipeline nữa.

### Quyết định

```text
MOVE OUT OF ACTIVE REPO
```

Ví dụ:

```text
../archive/steam_data/landing_filter_all_archive/
```

Không nên push GitHub.

---

# 13. `data/raw/review_pages/`

Nếu đây chỉ là các sample cũ:

```text
page_01.json
page_02.json
page_03.json
```

và production crawler không còn dùng thì:

```text
MOVE TO ARCHIVE
```

hoặc xóa nếu chắc chắn không cần.

---

# 14. Có cần `data/bronze/` local không?

Không cần nếu Data Lake chính thức nằm trên HDFS.

Hiện architecture đã chọn:

```text
HDFS Bronze
HDFS Silver
HDFS Gold
```

Không nên:

```text
local data/bronze
↓
copy HDFS bronze
```

vì sẽ duplicate dữ liệu.

### Quyết định

Nếu:

```text
data/bronze/
```

đang rỗng:

```text
DELETE
```

---

# 15. Có cần `data/silver/` local không?

Không cần nếu PySpark ghi trực tiếp Silver lên HDFS.

Flow đúng:

```text
HDFS Bronze
↓
PySpark
↓
HDFS Silver
```

Không cần:

```text
HDFS Bronze
↓
PySpark
↓
local Silver
↓
HDFS Silver
```

### Quyết định

Nếu rỗng:

```text
DELETE
```

---

# 16. Có cần `data/gold/` local không?

Tương tự Silver.

Gold chính thức nên nằm:

```text
HDFS Gold
```

Flow:

```text
HDFS Silver
↓
PySpark
↓
HDFS Gold
↓
├── Spark SQL
├── MLlib
└── MongoDB
```

### Quyết định

Nếu rỗng:

```text
DELETE
```

---

# 17. Vậy dữ liệu lớn nên nằm đâu?

## GitHub

Chỉ chứa:

```text
code
docs
README
requirements
schemas
queries
small evidence
metrics
```

Không chứa:

```text
raw review pages
large JSONL
Silver Parquet
Gold Parquet
HDFS files
```

---

## HDFS

Chứa Data Lake:

```text
/steam/
├── bronze/
├── silver/
└── gold/
```

Ví dụ:

```text
hdfs:///steam/bronze/
hdfs:///steam/silver/
hdfs:///steam/gold/
```

---

## Archive

Chứa backup local:

```text
archive/
└── steam_data/
    ├── landing_filter_all_archive/
    ├── old_review_pages/
    └── raw_backup/
```

---

# 18. Vì sao vẫn giữ raw backup?

HDFS hiện chạy local bằng Docker / OrbStack.

Nếu:

```text
docker volume bị xóa
docker compose down -v
HDFS corrupted
```

thì dữ liệu có thể mất.

Vì vậy nên có:

```text
HDFS
=
Working Data Lake

Archive
=
Backup
```

Không nên xem local HDFS là backup duy nhất.

---

# 19. `src/common/`

### KEEP

Chứa code dùng chung.

Ví dụ:

```text
config.py
jsonl.py
```

Vai trò:

```text
shared config
shared helpers
JSONL utilities
common constants
```

Không chứa business logic riêng cho từng phase.

---

# 20. `src/discovery/`

### KEEP

Dùng cho:

```text
Steam Catalog
↓
Candidate Discovery
↓
Qualification
```

Ví dụ:

```text
catalog_probe.py
catalog_qualify.py
review_qualify.py
```

Đây là phần giúp reproduce cách chọn game.

---

# 21. `src/selection/`

### KEEP

Flow:

```text
Candidates
↓
Selection Rules
↓
Selected 50 Games
```

Ví dụ:

```text
select_games.py
```

---

# 22. `src/ingestion/`

### KEEP

Đây là crawler chính.

Flow:

```text
Steam API
↓
Python Ingestion
↓
landing/
```

Ví dụ:

```text
prepare_game_metadata.py
review_crawler.py
```

---

# 23. `src/validation/`

### KEEP

Flow:

```text
landing
↓
validation
↓
bronze_ready
```

Ví dụ:

```text
audit_incomplete_games.py
finalize_review_sample.py
validate_bronze_ready.py
validate_landing.py
```

Vai trò:

- kiểm tra crawl
- kiểm tra completeness
- kiểm tra duplicate
- tạo finalization report
- tạo bronze-ready dataset

---

# 24. `src/hdfs/`

### KEEP

Dùng cho HDFS interaction / verification.

Ví dụ:

```text
verify_bronze.py
```

Flow:

```text
bronze_ready
↓
HDFS Bronze
↓
verify_bronze
```

Sau này có thể thêm:

```text
upload_bronze.py
verify_silver.py
verify_gold.py
```

nếu cần.

---

# 25. `src/batch/`

### KEEP

Đây là phase tiếp theo.

Sẽ chứa:

```text
bronze_to_silver.py
silver_to_gold.py
```

Flow:

```text
HDFS Bronze
↓
PySpark
↓
Silver
↓
Gold
```

Đây là phần Data Engineering chính của final.

---

# 26. `src/analytics/`

### KEEP

Dùng cho:

```text
Gold
↓
Spark SQL
↓
EDA
↓
Analytics
```

Ví dụ sau này:

```text
spark_analytics.py
```

Có thể chứa:

```text
genre recommendation rate
playtime analytics
free vs paid
game engagement
query plan
```

---

# 27. `src/ml/`

### KEEP

Dùng cho:

```text
Gold ML-ready
↓
Feature Engineering
↓
Spark MLlib
↓
Logistic Regression
Random Forest
↓
Evaluation
```

Có thể chứa:

```text
prepare_features.py
train_logistic_regression.py
train_random_forest.py
evaluate.py
```

---

# 28. `src/streaming/`

### KEEP

Dùng cho:

```text
Kafka
↓
Structured Streaming
↓
Silver / Gold Incremental Update
```

Có thể chứa:

```text
event_producer.py
schemas.py
stream_processor.py
```

Xử lý:

```text
schema validation
event_time
watermark
deduplication
checkpoint
state
```

---

# 29. `src/serving/`

### KEEP

Dùng cho MongoDB serving.

Flow:

```text
Gold Analytics
+
ML Predictions
↓
MongoDB
```

Có thể chứa:

```text
mongodb_writer.py
mongodb_queries.py
```

Collections dự kiến:

```text
game_summary
genre_analytics
model_predictions
```

---

# 30. `src/processing/`

### DELETE

Folder này trùng ý nghĩa với:

```text
batch/
analytics/
```

Nếu không chứa logic riêng thì không cần.

Nên xóa:

```bash
rm -rf src/processing
```

---

# 31. `__pycache__/`

### DELETE

Python tự sinh.

Không phải source code.

Có thể xóa:

```bash
find src -type d -name "__pycache__" -prune -exec rm -rf {} +
```

`.gitignore` phải có:

```gitignore
__pycache__/
*.pyc
```

---

# 32. `config/`

Nếu hiện tại rỗng:

```text
OPTIONAL
```

Nếu chưa có config riêng thì có thể xóa.

Sau này tạo lại khi cần:

```text
Spark config
Kafka config
Mongo config
HDFS path config
```

---

# 33. `scripts/`

Nếu rỗng:

```text
OPTIONAL
```

Sau này có thể chứa:

```text
run_batch.sh
run_streaming.sh
setup_hdfs.sh
run_ml.sh
```

Nếu chưa dùng thì có thể xóa.

---

# 34. `output/`

Không nên là nơi lưu Data Lake.

Nếu rỗng:

```text
DELETE
```

Nếu dùng cho:

```text
charts
temporary exports
local debug output
```

thì nên `.gitignore`.

Data chính vẫn nằm HDFS/MongoDB.

---

# 35. `evidence/`

### KEEP

Đây là folder rất quan trọng cho final.

Dùng chứa:

```text
HDFS evidence
MapReduce output
Spark SQL results
query plans
ML metrics
confusion matrix
MongoDB read-back
Streaming progress
checkpoint evidence
performance experiment
environment versions
```

Có thể chia:

```text
evidence/
├── hdfs/
├── mapreduce/
├── spark/
├── analytics/
├── ml/
├── streaming/
├── mongodb/
└── performance/
```

---

# 36. `docs/`

### KEEP

Dùng cho documentation.

Ví dụ:

```text
docs/
├── assets/
├── project-management/
├── 00_PROJECT_OVERVIEW.md
├── 01_ARCHITECTURE.md
├── 02_DATA_DICTIONARY.md
├── 03_DATA_PIPELINE.md
├── 04_MAPREDUCE_DESIGN.md
├── 05_SPARK_ANALYTICS.md
├── 06_STREAMING_DESIGN.md
├── 07_ML_DESIGN.md
├── 08_NOSQL_DESIGN.md
├── 09_DEPLOYMENT_AND_SCALABILITY.md
└── 10_TEAM_CONTRIBUTION.md
```

---

# 37. `docs/assets/`

### KEEP

Dùng cho:

```text
architecture images
diagrams
report figures
```

Ví dụ:

```text
steam-end-to-end-architecture.png
```

README sẽ reference ảnh từ đây.

---

# 38. Cấu trúc active repo nên hướng tới

```text
steam/
│
├── README.md
├── requirements.txt
├── .gitignore
│
├── docs/
│   ├── assets/
│   └── ...
│
├── evidence/
│
├── data/
│   └── raw/
│       ├── selected_50_games.jsonl
│       ├── bronze_ready/
│       └── landing/
│
└── src/
    ├── common/
    ├── discovery/
    ├── selection/
    ├── ingestion/
    ├── validation/
    ├── hdfs/
    ├── batch/
    ├── analytics/
    ├── streaming/
    ├── ml/
    └── serving/
```

---

# 39. Cấu trúc HDFS

Data Lake thật:

```text
HDFS
└── steam/
    ├── bronze/
    │   ├── games/
    │   ├── reviews/
    │   └── stream_events/
    │
    ├── silver/
    │   ├── games/
    │   └── reviews/
    │
    └── gold/
        ├── analytics/
        └── ml_ready/
```

---

# 40. Bronze trên HDFS

Bronze:

```text
raw
immutable
historical
replayable
```

Không clean mạnh.

Mục đích:

```text
Source of Truth
```

---

# 41. Silver trên HDFS

Silver:

```text
clean
typed
validated
deduplicated
normalized
```

Processing:

```text
schema casting
null handling
duplicate handling
timestamp conversion
key validation
normalization
```

Format ưu tiên:

```text
Parquet
```

---

# 42. Gold trên HDFS

Gold:

```text
business-ready
analytics-ready
ML-ready
```

Ví dụ Analytics Gold:

```text
game_summary
genre analytics
playtime analytics
free vs paid
engagement metrics
```

Ví dụ ML-ready Gold:

```text
features
label
player behavior
game metadata
encoded categorical fields
```

---

# 43. Folder nào có thể xóa ngay?

Nếu đang rỗng:

```text
data/bronze/
data/silver/
data/gold/
config/
scripts/
output/
```

Có thể xóa:

```text
src/processing/
src/**/__pycache__/
```

---

# 44. Folder nào nên move khỏi active repo?

Nên move:

```text
data/raw/landing_filter_all_archive/
data/raw/review_pages/
```

Sang:

```text
../archive/steam_data/
```

Không xóa ngay để tránh mất raw backup.

---

# 45. Folder nào nên giữ tạm?

Giữ:

```text
data/raw/landing/
data/raw/bronze_ready/
data/raw/selected_50_games.jsonl
```

Cho tới khi:

```text
HDFS Bronze verified
+
record count verified
+
backup ngoài repo đã tồn tại
```

Sau đó có thể move:

```text
landing/
bronze_ready/
```

ra archive.

---

# 46. Cleanup command đề xuất

Chỉ chạy sau khi xác nhận đúng project root.

Move archive lớn:

```bash
mkdir -p ../archive/steam_data

mv data/raw/landing_filter_all_archive \
  ../archive/steam_data/

mv data/raw/review_pages \
  ../archive/steam_data/ 2>/dev/null
```

Xóa local Data Lake folder rỗng:

```bash
rmdir data/bronze 2>/dev/null
rmdir data/silver 2>/dev/null
rmdir data/gold 2>/dev/null
```

Xóa processing folder thừa:

```bash
rm -rf src/processing
```

Xóa Python cache:

```bash
find src -type d -name "__pycache__" -prune -exec rm -rf {} +
```

Nếu các folder sau đang rỗng:

```bash
rmdir config 2>/dev/null
rmdir scripts 2>/dev/null
rmdir output 2>/dev/null
```

Không xóa:

```text
evidence/
docs/
src/batch/
src/analytics/
src/streaming/
src/ml/
src/serving/
```

vì đây là các workstream của final.

---

# 47. `.gitignore` nên đảm bảo có

```gitignore
# Python
.venv/
venv/
__pycache__/
*.pyc
*.pyo

# Secrets
.env
.env.*
!.env.example

# macOS
.DS_Store

# IDE
.idea/
.vscode/

# Raw / generated data
data/raw/
data/bronze/
data/silver/
data/gold/

# Generated output
output/

# Spark local artifacts
spark-warehouse/
metastore_db/
derby.log

# Large datasets
*.parquet
*.snappy.parquet

# Logs
*.log
logs/

# Local infrastructure data
kafka-data/
mongodb-data/
```

---

# 48. Vì sao không lưu Bronze/Silver/Gold cả local và HDFS?

Nếu lưu cả hai:

```text
Local Bronze
+
HDFS Bronze

Local Silver
+
HDFS Silver

Local Gold
+
HDFS Gold
```

thì:

```text
duplicate storage
khó biết đâu là source of truth
dễ lệch version
tốn disk
khó reproduce
```

Tốt hơn:

```text
Local
→ raw staging / backup

HDFS
→ authoritative Data Lake
```

---

# 49. Source of Truth theo từng giai đoạn

```text
Steam API
↓
raw source

landing/
↓
crawl staging

bronze_ready/
↓
validated upload package

HDFS Bronze
↓
Data Lake source of truth

HDFS Silver
↓
clean source for analytics

HDFS Gold
↓
business / ML source
```

---

# 50. Flow code theo folder

```text
src/discovery/
↓
find candidates

src/selection/
↓
choose 50 games

src/ingestion/
↓
crawl Steam

src/validation/
↓
validate landing
↓
bronze_ready

src/hdfs/
↓
upload / verify Bronze

src/batch/
↓
Bronze → Silver → Gold

src/analytics/
↓
EDA + Spark SQL

src/ml/
↓
MLlib

src/streaming/
↓
Kafka + Structured Streaming

src/serving/
↓
MongoDB
```

---

# 51. Trạng thái hiện tại

Đã hoàn thành:

```text
Discovery                  ✅
Qualification              ✅
Selection                  ✅
Steam Ingestion            ✅
Raw Validation             ✅
Bronze-ready               ✅
HDFS Bronze                ✅
Project Refactor           ✅
```

Bước tiếp theo:

```text
HDFS Bronze
↓
PySpark
↓
Silver Parquet
```

Sau đó:

```text
Silver
↓
Gold Base
↓
├── Analytics Gold
└── ML-ready Gold
```

---

# 52. Cấu trúc cuối cùng cần nhớ

```text
                    GIT
                     │
                Code / Docs
                     │
                     │
Steam API ──→ Local Landing
                     │
                  Validate
                     │
                Bronze-ready
                     │
                     ▼
                    HDFS
                     │
                  Bronze
                     │
                  PySpark
                     │
                  Silver
                     │
                  PySpark
                     │
                   Gold
          ┌──────────┼──────────┐
          ▼          ▼          ▼
      Spark SQL     MLlib     MongoDB
        EDA           ML       Serving
```

Streaming:

```text
ACTIVE games
     ↓
Steam API polling
     ↓
Detect changes / create events
     ↓
Kafka
     ↓
Structured Streaming
     ↓
Silver / Gold
```

Backup:

```text
Raw Crawl
↓
Archive outside Git repository
```

---

# 53. Nguyên tắc cuối cùng

## Git

```text
CODE
```

## HDFS

```text
DATA LAKE
```

## Archive

```text
RAW BACKUP
```

## Evidence

```text
PROOF FOR FINAL REPORT / DEMO
```

Đây là cách tổ chức nên giữ xuyên suốt project.
