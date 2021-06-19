from pyspark import SparkConf, SparkContext
sc = SparkContext(conf=SparkConf().setAppName("example").setMaster("yarn-client"))



import re
def parse_article(line: str):
    article_id, text = line.rstrip().lower().split('\t', 1)
    text = re.sub("^\W+|\W+$", "", text, flags=re.UNICODE)
    words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
    return words

stop_words = [word for word in sc.textFile("/data/wiki/stop_words_en-xpo6.txt").collect()]
def remove_stop_words(words):
    return list(filter(lambda x: x not in stop_words, words))



wiki = sc.textFile("hdfs:///data/wiki/en_articles_part/articles-part", 16).map(parse_article)
wiki = wiki.map(remove_stop_words).cache()

word_count = wiki.map(lambda a: len(a)).sum()
acticle_count = wiki.count()
pair_count = word_count - acticle_count

word_occurences = wiki.flatMap(lambda word_list: [(word, 1) for word in word_list]).reduceByKey(lambda a,b: a + b).cache()
word_probabilities = word_occurences.map(lambda pair: (pair[0], pair[1] / word_count)).cache()


result = word_probabilities.take(10)



def get_bigrams(words: list):
    return ['_'.join([a,b]) for a,b in zip(words[:-1], words[1:])]

bigrams = wiki.map(get_bigrams).cache()
bigram_count = bigrams.map(lambda a: len(a)).cache().sum()

bigram_occurences = bigrams.flatMap(lambda bigram_list: [(bigram, 1) for bigram in bigram_list])\
    .reduceByKey(lambda a,b: a + b).filter(lambda pair: pair[1] >= 500).cache()
bigram_probabilities = bigram_occurences.map(lambda pair: (pair[0], pair[1] / bigram_count)).cache()

result = bigram_occurences.take(10)



word_probs_as_map = sc.broadcast(word_probabilities.collectAsMap())

import math
def calc_PMI(bigram_probabylity):
    words = bigram_probabylity[0].split('_')
    fst_word_prob = word_probs_as_map.value[words[0]]
    scnd_word_prob = word_probs_as_map.value[words[1]]
#     snd_word_prob = word_probabilities.filter(lambda word: word == words[1]).take(1)[0][1]

    PMI = math.log(bigram_probabylity[1] / (fst_word_prob * scnd_word_prob))
    return (bigram_probabylity[0], PMI)

def calc_NPMI(bigram_probabylity):
    PMI = calc_PMI(bigram_probabylity)
    return (PMI[0], PMI[1] / (-math.log(bigram_probabylity[1])))

PMI = bigram_probabilities.map(calc_NPMI).sortBy(lambda pair: -pair[1])

result = PMI.take(39)
for pair in result:
    print(pair[0])
