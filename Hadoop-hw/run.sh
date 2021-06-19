#!/usr/bin/env bash

OUT_DIR="~/gyd-110-result"
NUM_REDUCERS=1

hdfs dfs -rm -r -skipTrash $OUT_DIR*

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="gyd-110" \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper.py,reducer.py \
    -mapper mapper.py \
        -reducer reducer.py \
    -input /data/ids \
    -output $OUT_DIR

# Checking result
for num in `seq 0 $(($NUM_REDUCERS - 1))`
do
    hdfs dfs -cat ${OUT_DIR}/part-0000$num | head
done

