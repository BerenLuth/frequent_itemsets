from __future__ import print_function
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.mllib.fpm import FPGrowth
import re


stopwordstxt = "stopword.txt"
large_file = '/srv/2015-01-08_geo_en_it_10M.plain.json'
sample_size = 0.05

#remove bad characters from text
def text_cleaner(text):
    text = re.sub('[^A-Za-z0-9@_]+|http.*|RT', ' ', text.lower())
    return text

#remove stopwords
def stopwords_remover(text_list):
        res = []
        for word in text_list:
                if len(word)>2:
                        if word not in stopwords:
                                res.append(word)
        return res

#keeps only one word's occurrence for basket
def duplicate_remover(text_list):
        return dict.fromkeys(text_list).keys()

#find text inside tweet's data and
def parse_text(tweet):
        #search text
        res = re.search('\"text\" : "(.*)" , \"in_reply_to_status_id\"', tweet).group(1)
        #remove bad characters
        res = text_cleaner(res)
        #remove words that appear more than once and next remove stopwords
        return stopwords_remover(duplicate_remover(res.split()))



spark = SparkSession.builder.appName("testFPGrowth").getOrCreate()
rdd_tweets = spark.textFile(large_file).sample(False, sample_size, 42).map(lambda x: parse_text(x))

model = FPGrowth.train(rdd_tweets, minSupport=0.02, numPartitions=1000)
result = model.freqItemsets().collect()
for fi in result:
    print(fi)
