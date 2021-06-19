import pyspark
from pyspark import SparkContext, SparkConf, SparkSession
import uuid
import numpy as np

# create the session
conf = SparkConf().set("spark.ui.port", "4050")

# create the context
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()


rdd = spark.sparkContext.textFile("/data/ids")
rdd2 = rdd.map(lambda x: x.strip().lower())
rdd_with_ids = rdd2.map(lambda x: (uuid.uuid4(), x))
sorted_rdd = rdd_with_ids.sortByKey(ascending=True).values()

count_ids_in_row = [np.random.randint(low=1, high=6) for i in range(50)]
ids_for_rows = sorted_rdd.take(300)


ptr = 0
for count in count_ids_in_row:
    for id in ids_for_rows[ptr:(ptr + count - 1)]:
        print(id, end=',')

    print(ids_for_rows[ptr + count - 1])
    ptr += count
