from pyspark import SparkContext, RDD
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import string
from kafka import KafkaProducer
import time
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('-n')
parser.add_argument('-keywords')
args = parser.parse_args()

n = int(args.n)
args_keywords = args.keywords.split(',')
keywords = args_keywords # ['lol', 'kek'] if len(args_keywords) <= 1 else args_keywords
print(keywords)
remove = dict.fromkeys(map(ord, '\n ' + string.punctuation))

sc = SparkContext(master='local[4]')
ssc = StreamingContext(sc, batchDuration=10)

producer = KafkaProducer(bootstrap_servers=['mipt-node06.atp-fivt.org:9092'],
                         value_serializer=lambda x:
                         x.encode('utf-8'))



dstream = KafkaUtils.createDirectStream(
    ssc, topics=['had2020011-topic'],
    kafkaParams = {'metadata.broker.list': 'mipt-node06.atp-fivt.org:9092'},
)

def send_list(lst):
    for word in lst:
        producer.send('had2020011-out', value=str(word))

def send_rdd(rdd):
    print('callllll')

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
        .updateStateByKey(aggregator, initialRDD=initState)

sortd = result.transform(lambda rdd: send_list(rdd.sortBy(lambda x: x[1], ascending=False).take(n)))
sortd.pprint(n)

ssc.checkpoint('./checkpoint{}'.format(time.strftime("%Y_%m_%d_%H_%M_%s", time.gmtime())))
ssc.start()
ssc.awaitTermination()


