from __future__ import print_function
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.mllib.fpm import FPGrowth
import re

tweet_sample = "sample.json"
stopwordstxt = "stopword.txt"


def tokenize(document):
	document = document.lower()
	document = re.sub('[!"#$%&\'()*+,-./:;<=>?@\[\\\\\]^_`{|}~]', '', document)
	return document.strip().split(" ")


spark = SparkSession.builder.appName("testFPGrowth").getOrCreate()
rdd_tweets = spark.read.json(tweet_sample).rdd.map(lambda x: tokenize(x[23]))

model = FPGrowth.train(rdd_tweets, minSupport=0.02, numPartitions=1000)
result = model.freqItemsets().collect()
for fi in result:
    print(fi)

