#!/bin/bash


INPUT=/steam/bronze/reviews
OUTPUT=/steam/gold/mapreduce_game_review


hdfs dfs -rm -r $OUTPUT


hadoop jar \
$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar \
\
-input $INPUT \
-output $OUTPUT \
\
-mapper mapper.py \
-reducer reducer.py \
\
-file mapper.py \
-file reducer.py