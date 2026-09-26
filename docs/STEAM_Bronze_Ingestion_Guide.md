# Steam Big Data Platform - Bronze Ingestion Guide

## 1. Overview

This guide explains how to run the Bronze ingestion pipeline.

The pipeline:
- Reads raw Steam JSONL data from HDFS Bronze layer
- Applies explicit PySpark schemas
- Captures raw counts and schemas
- Writes Bronze Parquet output

Flow:

Raw JSONL -> HDFS Bronze -> PySpark Ingestion -> Bronze Parquet


## 2. Input Data

HDFS input paths:

Games:
```
/user/bda501/steam/bronze/games
```

Reviews:
```
/user/bda501/steam/bronze/reviews
```


## 3. Schema Definition

### Games

Important fields:
- appid: integer
- name: string
- developers: array<string>
- publishers: array<string>
- genres: array<string>
- categories: array<string>
- is_free: boolean
- release_date: string
- price_overview: struct


### Reviews

Important fields:
- recommendationid: string
- appid: integer
- author: struct
- language: string
- review: string
- voted_up: boolean
- votes_up: integer
- weighted_vote_score: double
- steam_purchase: boolean
- received_for_free: boolean


## 4. Start Environment

From project root:

```
cd docker/hadoop
docker compose up -d
```

Check containers:

```
docker ps
```

Expected:
- steam-namenode
- steam-datanode
- steam-spark


## 5. Upload Data To HDFS

Enter namenode:

```
docker exec -it steam-namenode bash
```

Create directories:

```
hdfs dfs -mkdir -p /user/bda501/steam/bronze/games
hdfs dfs -mkdir -p /user/bda501/steam/bronze/reviews
```

Upload games:

```
hdfs dfs -put data/raw/steam/landing/games/*.jsonl /user/bda501/steam/bronze/games
```

Upload reviews:

```
hdfs dfs -put data/raw/steam/landing/reviews_by_game/*.jsonl /user/bda501/steam/bronze/reviews
```

Verify:

```
hdfs dfs -ls -R /user/bda501/steam/bronze
```


## 6. Run Spark Bronze Ingestion

Enter Spark:

```
docker exec -it steam-spark bash
```

Run:

```
/opt/spark/bin/spark-submit \
--master local[*] \
--py-files /workspace/src \
/workspace/src/processing/bronze_ingestion.py
```


## 7. Expected Output

Games:

```
Games count: 50
```

Schema example:

```
appid: integer
name: string
developers: array
genres: array
```


Reviews:

```
Reviews count: xxxx
```

Schema example:

```
recommendationid: string
appid: integer
author: struct
voted_up: boolean
```


## 8. Output Parquet

Generated files:

```
/user/bda501/steam/bronze/parquet/games
/user/bda501/steam/bronze/parquet/reviews
```

Verify:

```
hdfs dfs -ls -R /user/bda501/steam/bronze/parquet
```


## 9. Troubleshooting

### ModuleNotFoundError: schemas

Cause:
Spark cannot find source modules.

Solution:

Use:

```
--py-files /workspace/src
```


### hdfs command not found

Cause:
Running HDFS command inside Spark container.

Solution:

Run HDFS commands inside:

```
docker exec -it steam-namenode bash
```


## 10. Completed Tasks

Bronze ingestion completed:

[x] Define explicit PySpark schemas
[x] Read HDFS Bronze data
[x] Capture raw counts
[x] Capture DataFrame schema
[x] Write Bronze Parquet output


Next step:
Silver cleaning and transformation pipeline.
