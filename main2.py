from pyspark.sql import SparkSession
import re

small_file = '/srv/sample.json'
large_file = '/srv/2015-01-08_geo_en_it_10M.plain.json'
tweets =

threshold = 3
#TO BE IMPLEMENTED
def parse_text(tweet):
    #sistemare con i simboli
    #return tweet[23].split()
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
    text = parse_text(tweet)
    for x in text:
        y = next(x)
        while (y != None):
            if (x in tweets && y in tweets):
                resultlist.append((x,y))
            y = next(y)
    return resultlist

if __name__ == '__main__':
    print "\n### main.py ###\n"

    #create spark context
    spark_session = SparkSession.builder.appName("PythonSON").getOrCreate()

    #read tweets file (sample or complete) and parse text from every tweet
    #maybe with parallelize e list
    tweets = spark_session.read.json(small_file).rdd.flatMap(lambda x: parse_text(x)).countByValue()
    for key, value in tweet.items():
        if value < threshold:
            del tweets[key]


    bucket = spark_session.read.text(small_file).rdd.flatMap(lambda x: nested_loop(x))


    #print tweets
    print_dict(tweets)
