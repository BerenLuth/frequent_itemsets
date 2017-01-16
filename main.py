from pyspark.sql import SparkSession
import re

small_file = '/srv/sample.json'
large_file = '/srv/2015-01-08_geo_en_it_10M.plain.json'

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

if __name__ == '__main__':
    print "\n### main.py ###\n"

    #create spark context
    spark_session = SparkSession.builder.appName("PythonSON").getOrCreate()

    #read tweets file (sample or complete) and parse text from every tweet
    #maybe with parallelize e lista
    tweets = spark_session.read.json(small_file).rdd.flatMap(lambda x: parse_text(x)).countByValue()

    #print tweets

    print_dict(tweets)
