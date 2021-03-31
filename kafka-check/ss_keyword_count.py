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

producer = KafkaProducer(bootstrap_servers=['mipt-node06.atp-fivt.org:9092'],
                         value_serializer=lambda x:
                         x.encode('utf-8'))

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "mipt-node06.atp-fivt.org:9092,mipt-node04.atp-fivt.org:9092")\
    .option("subscribe", "simple-input-had2020011").option("startingOffsets", "earliest").load()
values = df
values.printSchema()

import sys
keywords = ['lol', 'kek'] if len(sys.argv) <= 1 else sys.argv[1:]



# def process_data(keyword, x):
#     result = []
#     for word in str(x).split():
#         translated_word = word.strip(''.join(string.punctuation))
#
#         if translated_word == keyword:
#             result.append(translated_word)
#
#     return ' '.join(result)

from functools import partial

# process_values = lambda keyword : udf(partial(process_data, keyword), StringType())
# processed_values = values.withColumn("value", process_values('lol')("value").alias('processed_value'))

# counts = values.withWatermark('timestamp', '1 minutes').groupBy(
#     window(col("timestamp"), '1 minute'),
#     col("value")
# ).agg(count(col('value')))

producer = KafkaProducer(bootstrap_servers=['mipt-node06.atp-fivt.org:9092'],
                         value_serializer=lambda x:
                         x.encode('utf-8'))

producer.send('simple-output-had2020011', str(values.withWatermark("timestamp", "1 minute").groupBy(
    window('timestamp', '1 minute', '1 minute'),
    'value'
).count()))# .select('value', col('count').alias('count'))

counted = values.withWatermark("timestamp", "1 minute").groupBy(
    window('timestamp', '1 minute', '1 minute'),
    'value'
).count().select('value')

counted.writeStream \
    .format("kafka").option("kafka.bootstrap.servers", "mipt-node06.atp-fivt.org:9092,mipt-node04.atp-fivt.org:9092") \
    .option('topic', 'simple-output-had2020011').option("checkpointLocation", "check1") \
    .start().awaitTermination()
