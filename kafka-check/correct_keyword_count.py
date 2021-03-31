from pyspark import SparkContext, RDD
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import string
from kafka import KafkaProducer
import time


sc = SparkContext(master='local[4]')
ssc = StreamingContext(sc, batchDuration=60)

producer = KafkaProducer(bootstrap_servers=['mipt-node06.atp-fivt.org:9092'],
                         value_serializer=lambda x:
                         x.encode('utf-8'))



dstream = KafkaUtils.createDirectStream(
    ssc, topics=['had2020011-topic'],
    kafkaParams = {'metadata.broker.list': 'mipt-node06.atp-fivt.org:9092'}
)

import sys
keywords = ['lol', 'kek'] if len(sys.argv) <= 1 else sys.argv[1:]
remove = dict.fromkeys(map(ord, '\n ' + string.punctuation))

def send_rdd(rdd):
    out_list = rdd.collect()
    for word in out_list:
        producer.send('had2020011-out', value=str(word))

initialized = False

def aggregator(values, old):
    return (old or 0) + sum(values)

initState = sc.parallelize(list(zip(keywords, [0] * len(keywords))))

result = dstream \
        .flatMap(lambda pair: pair[1].split(" ")) \
        .map(lambda word: word.translate(remove)) \
        .filter(lambda word: word in keywords) \
        .map(lambda word: (word.lower(), 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .updateStateByKey(aggregator, initialRDD=initState) \
        .foreachRDD(lambda rdd : send_rdd(rdd))


ssc.checkpoint('./checkpoint{}'.format(time.strftime("%Y_%m_%d_%H_%M_%s", time.gmtime())))
ssc.start()
ssc.awaitTermination()


