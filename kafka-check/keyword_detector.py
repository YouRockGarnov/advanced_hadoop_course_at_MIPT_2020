from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import string
from kafka import KafkaProducer
from pyspark.sql.types import *
from pyspark.sql.functions import col, asc, udf

spark = SparkSession.builder.master('local[4]').getOrCreate()
# ssc = StreamingContext(spark.sparkContext, batchDuration=10)

producer = KafkaProducer(bootstrap_servers=['mipt-node06.atp-fivt.org:9092'],
                         value_serializer=lambda x:
                         x.encode('utf-8'))

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "mipt-node06.atp-fivt.org:9092,mipt-node04.atp-fivt.org:9092")\
    .option("subscribe", "simple-input-had2020011").option("startingOffsets", "earliest").load()
values = df.select('value')

import sys
keywords = ['lol', 'kek'] if len(sys.argv) <= 1 else sys.argv[1:]

def process_data(x):
    result = []
    for word in str(x).split():
        translated_word = word.strip(''.join(string.punctuation))

        if translated_word in keywords:
            result.append(translated_word)

    return ' '.join(result)

process_values = udf(process_data, StringType())
processed_values = values.withColumn("value", process_values("value").alias('processed_value'))

processed_values.writeStream \
    .format("kafka").option("kafka.bootstrap.servers", "mipt-node06.atp-fivt.org:9092,mipt-node04.atp-fivt.org:9092") \
        .option('topic', 'simple-output-had2020011').option("checkpointLocation", "checkpoint") \
    .start().awaitTermination()
