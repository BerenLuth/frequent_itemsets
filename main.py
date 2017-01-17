from pyspark.sql import SparkSession
from pyspark.sql import functions
import re

small_file = '/srv/sample.json'
large_file = '/srv/2015-01-08_geo_en_it_10M.plain.json'

threshold = 10



def text_cleaner(text):
    text = re.sub('[^A-Za-z0-9@_]+|http.*|RT', '', text.lower())
    return text

#TO BE IMPLEMENTED
def parse_text_json(tweet):
    #return text param from tweet
    res = []
    for t in tweet[23].split():
        t = text_cleaner(t)
        if not t=='':
                res.append(t)
    return res

def print_dict(mydict):
    boh = sorted(mydict, key=mydict.get, reverse=True)
    print "\n\tTOP 30 ABSOLUTE\n"
    count = 1
    for x in boh[0:30]:
       print str(count) + "\t" + str(x) + ": " + str(mydict[x])
       count +=1

    print "\n"


def alphabet_ordered(a,b):
        if a<b:
                return (a,b)
        else:
                return (b,a)

def nested_loop(tweet):
    resultlist = []
    text = parse_text_json(tweet)
    x = y = 0
    while x < len(text):
        y = x+1
        if text[x] in tweets:
                while y < len(text):
                        if text[y] in tweets and not text[y]==text[x]:
                                resultlist.append(alphabet_ordered(text[x],text[y]))
                        y += 1
        x += 1
    return resultlist

def filter_dict(my_dict):
    for key, value in my_dict.items():
        if value < threshold:
            del my_dict[key]

if __name__ == '__main__':
    print "\n### main.py ###\n"

    #create spark context
    spark_session = SparkSession.builder.appName("PythonSON").getOrCreate()

    #read tweets file (sample or complete) and parse text from every tweet
    tweets = spark_session.read.json(small_file).rdd.flatMap(lambda x: parse_text_json(x)).countByValue()

    filter_dict(tweets)
    #tweets.filter(lambda x: tweets[x]>threshold)

    #print tweets

    bucket = spark_session.read.json(small_file).rdd.flatMap(lambda x: nested_loop(x)).countByValue()

    filter_dict(bucket)

    #print bucket
    print_dict(bucket)


