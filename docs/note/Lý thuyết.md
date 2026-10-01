# 1 Toàn bộ project

					STEAM API
                       │
            ┌──────────┴──────────┐
            │                     │
     Historical / Batch      New data liên tục
            │                     │
            │                  Producer
            │                     │
            │                   Kafka
            │                     │
            ▼                     ▼
       HDFS BRONZE         Structured Streaming
       Raw / replay                │
            │                    │
            └──────────┬─────────┘
                       ▼
                    SILVER
               Clean / Normalize
                       │
                       ▼
                     GOLD
              Join / Features / KPI
                  /          \
                 /            \
          Spark SQL           MLlib
          Analytics            ML
                 \
                  \
                 MongoDB
             Serving / API


# 2. Batch processing

**Batch = gom một lượng dữ liệu rồi xử lý một lần.**
Baseline ban đầu:
```
50 games
25,000 reviews
```

Sau Dynamic Onboarding batch đầu tiên, snapshot hiện tại là 60 games và 30.000
reviews. Batch processing không phụ thuộc một con số cố định.

Ví dụ baseline chạy Spark:
25,000 Bronze Reviews
        ↓
     PySpark
        ↓
25,000 Silver Reviews
        ↓
      Gold
        ↓
25,000 Gold records
=> Đây là **batch processing**.


Batch rất phù hợp với:
- historical data;
- backfill;
- ETL định kỳ;
- dữ liệu không cần phản hồi tức thời;
- training ML;
- báo cáo ngày/tuần/tháng.


# 3. Streaming
Streaming xử lý **dữ liệu mới liên tục khi nó xuất hiện**, thay vì đợi gom thành toàn bộ dataset.

Khi Steam xuất hiện review mới:

```
Review A
Review B
Review C
Review D
...
```

Producer liên tục lấy dữ liệu:

```
Steam API
   ↓
Producer
   ↓
Kafka
   ↓
Structured Streaming
   ↓
Silver / Gold
```

Bạn không cần chạy lại toàn bộ historical snapshot mỗi khi có thêm vài review.

Đây là lý do project Big Data cần streaming khi dữ liệu phát sinh liên tục.


# 4. Batch vs Streaming

|            | Batch                  | Streaming                          |
| ---------- | ---------------------- | ---------------------------------- |
| Data       | Một khối dữ liệu       | Dữ liệu liên tục                   |
| Xử lý      | Theo đợt               | Liên tục/gần realtime              |
| Latency    | Phút → giờ             | ms → giây/phút                     |
| Complexity | Thấp hơn               | Cao  hơn                           |
| Replay     | Dễ                     | Cần checkpoint/event log           |
| Hiện tại   | Backfill/rebuild snapshot | Review mới từ ACTIVE games      |
| Công nghệ  | Spark Batch, MapReduce | Kafka + Spark Structured Streaming |

# 5 Micro-batch
Đây là phần rất hay bị nhầm.

Streaming không nhất thiết nghĩa là:
```
1 record tới
→ xử lý ngay lập tức record đó
```

Spark Structured Streaming thường xử lý theo kiểu **micro-batch**.
Ví dụ Kafka nhận:
```
10:00:00 review A
10:00:01 review B
10:00:02 review C
10:00:03 review D
10:00:04 review E
```

Spark có thể gom:
```
10:00:00 → 10:00:05
A B C D E
```
rồi xử lý một batch nhỏ.

Sau đó:
```
10:00:05 → 10:00:10
F G H ...
```

Tức là:
```
Batch truyền thống

[      25,000 records      ]
             ↓
           Spark
```

so với:
```
Micro-batch streaming

[A B C] → Spark
[D E]   → Spark
[F G H] → Spark
...
```

Nó vẫn là batch, nhưng **batch rất nhỏ và chạy liên tục**.

Đây là lý do gọi:
> micro-batch streaming.


# 6 Kafka
Kafka không phải database phân tích và cũng không thay HDFS.

Kafka chủ yếu là **event/message broker + distributed log**.

Ví dụ:

```
Steam API
   ↓
Producer
   ↓
Kafka Topic: steam_events
   ↓
Review 1
Review 2
Review 3
Review 4
...
```

Consumer có thể là:

```
                   Kafka
                  /     \
                 /       \
       Structured        Raw archiver
        Streaming            │
            │                 ▼
            ▼             HDFS Bronze
         Silver
```

Kafka giải quyết chuyện:

> “Dữ liệu đang chạy liên tục thì làm sao các hệ thống nhận được nó?”

HDFS giải quyết chuyện:

> “Dữ liệu khối lượng lớn sẽ lưu lâu dài ở đâu?”

Spark giải quyết:

> “Dữ liệu đó được xử lý như thế nào?”

Ba cái làm ba việc khác nhau.


# 7. HDFS

HDFS = **Hadoop Distributed File System**.

Nó là filesystem phân tán.

Không phải database kiểu:

```
SELECT * FROM reviews;
```

bản thân HDFS không tập trung vào query.

HDFS tập trung vào:

```
Lưu file rất lớn
Phân tán file thành blocks
Đặt blocks trên nhiều DataNode
Replication
Fault tolerance
High throughput
```

Ví dụ sau này:

```
1 TB Steam Reviews
```

không phải một file bắt buộc nằm trên một máy.

Có thể:

```
File 1 TB
   ↓ split blocks

Block 1 → DataNode A
Block 2 → DataNode B
Block 3 → DataNode C
...
```

Trong lab của bạn hiện chỉ có:

```
1 DataNode
replication = 1
```

nên chưa thật sự distributed nhiều node, nhưng **mô hình kiến trúc vẫn giống cluster lớn**.

# 8. Tại sao không để file local?

Đây chính là câu bạn vừa hỏi trước đó.

Local:

```
MacBook
└── data/
    └── 25k reviews
```

ổn để:

```
development
debug
temporary staging
small experiments
```

Nhưng nếu:

```
25k
→ 1 triệu
→ 100 triệu
→ 5 TB
```

thì local trở thành bottleneck.

HDFS cho phép:

```
Storage scale horizontally
```

nghĩa là thay vì mua một máy cực lớn:

```
1 máy 50 TB
```

có thể:

```
Node A 10 TB
Node B 10 TB
Node C 10 TB
Node D 10 TB
Node E 10 TB
```

---

# 9. Spark
Spark là **distributed computation engine**.
Nói đơn giản:
> HDFS giữ dữ liệu, Spark xử lý dữ liệu.

Spark không chỉ là ETL.
Nó có cả:
```
Spark Core
Spark SQL
DataFrame
Structured Streaming
MLlib
```
Trong bài:
```
PySpark
├── Bronze → Silver
├── Silver → Gold
├── Spark SQL / EDA
├── Structured Streaming (đã triển khai cho REVIEW_CREATED)
└── MLlib (chưa hoàn tất)
```
Nên Spark gần như là **processing backbone** của project.


# 10. ETL là gì?
ETL:
```
Extract
Transform
Load
```

Trong project:
### Extract
```
Steam API
   ↓
JSON
```

### Transform
```
nested raw JSON
     ↓
flatten
clean
cast
normalize
derive features
```

### Load
```
Silver Parquet
Gold Parquet
HDFS
```

Flow:
```
Steam API
   ↓ Extract

Bronze
   ↓ Transform

Silver
   ↓ Transform/join

Gold
   ↓ Load/use
```

Nhưng hiện đại còn có ELT:

```
Extract
Load
Transform
```

Data lake thường khá giống ELT:

```
Steam API
   ↓
Load raw vào Bronze trước
   ↓
Transform sau
```

Nên project của bạn về mặt kiến trúc thực tế gần:

> **ELT/Data Lake pattern**

hơn ETL truyền thống thuần túy.


# 11 MapReduce
MapReduce là model xử lý distributed đời Hadoop.

Có hai giai đoạn chính:

```
MAP
 ↓
key/value
 ↓
shuffle/sort
 ↓
REDUCE
```

Ví dụ tính recommendation theo game.

Input:

```
Dota,true
Dota,false
Dota,true
CS2,true
```

Map:

```
Dota → (1,1)
Dota → (1,0)
Dota → (1,1)
CS2  → (1,1)
```

Shuffle/group:

```
Dota → [(1,1),(1,0),(1,1)]
CS2  → [(1,1)]
```

Reduce:

```
Dota → total=3 positive=2
CS2  → total=1 positive=1
```


# 12 MapReduce vs Spark

|                   | MapReduce            | Spark                |
| ----------------- | -------------------- | -------------------- |
| Generation        | Older Hadoop         | Modern               |
| Processing        | Map → disk → Reduce  | DAG/memory optimized |
| Intermediate data | Nhiều disk I/O       | Có thể cache memory  |
| Iterative ML      | Chậm                 | Tốt hơn              |
| API               | Low-level            | DataFrame/SQL dễ hơn |
| Streaming         | Không phải điểm mạnh | Structured Streaming |
| Project           | Validation           | Main processing      |
Spark = main ETL/analytics engine

MapReduce = validation / comparison




# 13 HDFS local thực sực có tác dụng
Dữ liệu cuối cùng vẫn nằm ở ổ cứng vật lý của Mac
Lấy dung lượng máy tạo các DataNode

HDFS local của bạn hiện chủ yếu giúp học:

```
HDFS architecture
NameNode
DataNode
blocks
replication
HDFS path
Spark ↔ HDFS
```

# 14 Khi nào HDFS mới thật sự tăng dung lượng
Khi các DataNode nằm trên **máy vật lý khác nhau**.

Ví dụ:

```
Machine A
DataNode
2 TB

Machine B
DataNode
4 TB

Machine C
DataNode
4 TB
```

Cluster có tổng physical storage khoảng:

```
2 + 4 + 4 = 10 TB
```

trước khi tính ảnh hưởng của replication.

Ví dụ replication = 3 thì cùng một block có thể được lưu 3 bản, nên usable capacity giảm xuống.

Trong lab bạn:

```
1 DataNode
replication = 1
```

là hợp lý.


# 15 Khi tạo DataNode mới thì HDFS tự chuyển data qua không?
Ta có, DataNode A có Block 1,2,3,4,5. Thêm DataNode B
HDFS sẽ nhận biết B đã join cluster và **các write mới có thể được NameNode đặt lên node mới**. chứ ko ngay lập tức HDFS chia lại 50% dữ liệu từ A qua B

Trong dữ liệu cũ, HDFS có thể cân bằng giữa các DataNode bằng cách chuyển block

# 16 NameNode, DataNode
Không phải nội dung file chính.

NameNode chủ yếu giữ:

```
metadata
```

ví dụ:

```
/steam/bronze/reviews/570.jsonl

→ file gồm Block A, Block B
→ Block A nằm DataNode 1
→ Block B nằm DataNode 3
```

DataNode mới thực sự giữ:

```
block bytes
```

Mental model:

```
NameNode
= bản đồ / directory / quản lý

DataNode
= kho chứa dữ liệu thật
```



# 17 Tại sao Bronze JSON mà Silver/Gold phải ra Parquet?
Đây là một quyết định rất quan trọng.

Bronze muốn:

```
raw
gần dữ liệu nguồn
dễ replay
```

nên JSON hợp lý.

Nhưng analytics không nên cứ đọc JSON mãi.

Ví dụ JSON:

```
{
  "appid": 570,
  "game_name": "Dota 2",
  "voted_up": true,
  "playtime": 5000,
  "price": 0
}
```

Nếu có 100 triệu record, JSON phải parse text liên tục.

Parquet khác hoàn toàn

# 18 Parquet là columnar format

Ví dụ table:

```
appid | name | voted_up | playtime | price
------------------------------------------------
570   | Dota | true     | 100      | 0
730   | CS2  | false    | 300      | 0
...
```

Row-oriented có xu hướng lưu:

```
570,Dota,true,100,0
730,CS2,false,300,0
```

Parquet lưu gần theo column:

```
appid:
570,730,...

name:
Dota,CS2,...

voted_up:
true,false,...

playtime:
100,300,...
```

Giờ query của bạn chỉ cần:

```
SELECT game_name, voted_up
FROM gold
```

Spark không cần đọc hết:

```
playtime
price
categories
platforms
...
```

Đây gọi là:

```
column pruning
```
