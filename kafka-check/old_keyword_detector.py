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

# df = KafkaUtils.createDirectStream(
#     ssc, topics=['had2020011-topic'],
#     kafkaParams = {'metadata.broker.list': 'mipt-node06.atp-fivt.org:9092'}
# )

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "mipt-node06.atp-fivt.org:9092,mipt-node04.atp-fivt.org:9092")\
    .option("subscribe", "simple-input-had2020011").option("startingOffsets", "earliest") \
    .load()
    # .filter(lambda word: word in keywords)
    # .foreach(lambda pair: pair[1].split(" "))

import sys
keywords = ['lol', 'kek'] if len(sys.argv) <= 1 else sys.argv[1:]


df.printSchema()
values = df.select('value') # .rdd # .filter(col('value').isin(keywords))
# values = spark.createDataFrame(rdd_values)
# values.toDF()

def process_data(x):
    result = []
    for word in str(x).split():
        translated_word = word.strip(''.join(string.punctuation))

        if translated_word in keywords:
            result.append(translated_word)

    return ' '.join(result)

strip_values = udf(process_data, StringType())

processed_values = values.withColumn("value", strip_values("value").alias('processed_value'))

processed_values.writeStream \
    .format("kafka").option("kafka.bootstrap.servers", "mipt-node06.atp-fivt.org:9092,mipt-node04.atp-fivt.org:9092") \
    .option('topic', 'simple-output-had2020011').option("checkpointLocation", "checkpoint") \
    .start().awaitTermination()


# main = df.writeStream.format("kafka").option("kafka.bootstrap.servers", "mipt-node06.atp-fivt.org:9092,mipt-node04.atp-fivt.org:9092") \
#     .option('topic', 'simple-output-had2020011').option("checkpointLocation", "checkpoint") \
#     .start().awaitTermination()

# .foreach(lambda pair: pair[1].split(" ")) \
#     .map(lambda word: word.translate(remove)) \
#     .filter(lambda word: word in keywords) \
#     .map(lambda word: word.lower()) \

# .option("startingOffsets", "beginning") \






def send_rdd(rdd):
    if rdd.isEmpty():
        return

    df = spark.createDataFrame(rdd)

    df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "mipt-node06.atp-fivt.org:9092") \
        .option("had2020011-out") \
        .start()

# result = df \
#         .foreach(lambda pair: pair[1].split(" ")) \
#         .map(lambda word: word.translate(remove)) \
#         .filter(lambda word: word in keywords) \
#         .map(lambda word: word.lower()) \
#         .writeStream \
#         .format("console").start().awaitTermination()
        # .foreachRDD(lambda rdd : send_rdd(rdd))


# ssc.start()


