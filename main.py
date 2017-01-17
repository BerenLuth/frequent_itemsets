from pyspark.sql import SparkSession
from pyspark.sql import functions
import re

small_file = '/srv/sample.json'
large_file = '/srv/2015-01-08_geo_en_it_10M.plain.json'

threshold = 3

#TO BE IMPLEMENTED
def parse_text_json(tweet):
    #return text param from tweet
    return tweet[23].split()

def print_dict(mydict):
    boh = sorted(mydict, key=mydict.get, reverse=True)
    print "\n\tTOP 30 ABSOLUTE\n"
    count = 1
    for x in boh[0:30]:
       print str(count) + "\t" + x + ": " + str(mydict[x])
       count +=1

    print "\n"

def nested_loop(tweet):
    resultlist = []
    text = parse_text_json(tweet)
    x = y = 0
    while x < len(text):
        y = x+1
        if text[x] in tweets:
                while y < len(text):
                        if text[y] in tweets:
                                resultlist.append((text[x],text[y]))
                        y += 1
        x += 1
    return resultlist

if __name__ == '__main__':
    print "\n### main.py ###\n"

    #create spark context
    spark_session = SparkSession.builder.appName("PythonSON").getOrCreate()

    #read tweets file (sample or complete) and parse text from every tweet
    tweets = spark_session.read.json(small_file).rdd.flatMap(lambda x: parse_text_json(x)).countByValue()

    for key, value in tweets.items():
        if value < threshold:
            del tweets[key]

    #print tweets

    bucket = spark_session.read.json(small_file).rdd.flatMap(lambda x: nested_loop(x))

    print bucket.collect()

