from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import string
from kafka import KafkaProducer
from pyspark.sql.types import *
from pyspark.sql.functions import col, asc, udf, window, count

spark = SparkSession.builder.master('local[4]').getOrCreate()
# ssc = StreamingContext(spark.sparkContext, batchDuration=10)

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "mipt-node06.atp-fivt.org:9092,mipt-node04.atp-fivt.org:9092")\
    .option("subscribe", "simple-input-had2020011").option("startingOffsets", "earliest").load()
values = df
values.printSchema()

import sys
keywords = ['lol', 'kek'] if len(sys.argv) <= 1 else sys.argv[1:]


def process_data(keyword, x):
    result = []
    for word in str(x).split():
        translated_word = word.strip(''.join(string.punctuation))

        if translated_word == keyword:
            result.append(translated_word)

    return ' '.join(result)

# from functools import partial
# process_values = lambda keyword : udf(partial(process_data, keyword), StringType())
# processed_values = values.withColumn("value", process_values('lol')("value").alias('processed_value'))


process_values = udf(process_data, StringType())
processed_values = values.withColumn("value", process_values("value").alias('processed_value'))

counts = processed_values.withWatermark('timestamp', '1 minutes').groupBy(
    window(col("timestamp"), '1 minute'),
    col("processed_value")
).agg(count(col('processed_value')).alias('count'))

counts.writeStream \
    .format("kafka").option("kafka.bootstrap.servers", "mipt-node06.atp-fivt.org:9092,mipt-node04.atp-fivt.org:9092") \
    .option('topic', 'simple-output-had2020011').option("checkpointLocation", "check1") \
    .start().awaitTermination()
