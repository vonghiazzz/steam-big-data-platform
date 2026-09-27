# Steam Big Data Platform - Bronze Ingestion Guide

1. Overview
Bronze ingestion pipeline:

Raw Steam JSONL -> HDFS Bronze -> PySpark Ingestion -> Bronze Parquet

Completed:
- Explicit PySpark schemas for games and reviews
- Read HDFS Bronze JSONL
- Capture raw counts and schemas
- Write Bronze Parquet output

Final Bronze paths:
- /steam/bronze/games
- /steam/bronze/reviews
2. Input Data
HDFS input paths:

Games:
/steam/bronze/games

Reviews:
/steam/bronze/reviews

The previous path:
/user/bda501/steam/bronze
is no longer used.
3. Schema Definition
Raw structures:

Games:
- data.name
- data.genres
- data.is_free
- data.developers
- data.publishers
- data.categories

Reviews:
- review.recommendationid
- review.author
- review.voted_up
- review.review

Schema file:
src/schemas/steam_bronze_schema.py
4. Bronze Ingestion Script
Keep only one ingestion script:

src/bronze/ingest_steam_bronze.py

Remove duplicated files:
- src/processing/bronze_ingestion.py
- docker/hadoop/workspace/bronze_ingestion.py
5. Start Environment
cd docker/hadoop

docker compose up -d

Check:

docker ps

Expected:
- steam-namenode
- steam-datanode
- steam-spark
6. Verify HDFS Bronze
Enter namenode:

docker exec -it steam-namenode bash

Check:

hdfs dfs -ls -R /steam/bronze

Verified:
- 50 games
- 25,000 reviews
7. Run Bronze Ingestion
Enter Spark:

docker exec -it steam-spark bash

cd /workspace

export PYTHONPATH=/workspace/src

Run:

/opt/spark/bin/spark-submit \
src/bronze/ingest_steam_bronze.py
8. Expected Output
Games count: 50

Reviews count: 25000

Example schema:

Games:
appid integer
name string
genres array<string>
is_free boolean

Reviews:
appid integer
review struct
recommendationid string
author struct
voted_up boolean
9. Output Parquet
Output:

/steam/bronze/parquet/games

/steam/bronze/parquet/reviews

Verify:

hdfs dfs -ls -R /steam/bronze/parquet
10. Troubleshooting
ModuleNotFoundError: schemas

Solution:
export PYTHONPATH=/workspace/src


hdfs command not found

Run HDFS commands inside:
docker exec -it steam-namenode bash


PATH_NOT_FOUND

Check:
hdfs dfs -ls /steam/bronze
11. Completed Tasks
[x] Bronze paths finalized
[x] Explicit PySpark schemas
[x] Read HDFS Bronze data
[x] Capture counts and schema
[x] Write Bronze Parquet

Next:
Silver cleaning and transformation:
- type casting
- null handling
- deduplication
- timestamp normalization
- derived fields (price, playtime_hours, normalized categories)
