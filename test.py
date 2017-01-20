from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark import SparkContext
import re

name = "Se ci mette piu' di 30 minuti non killate comunque perche' 25 sono di coda"

small_file = '/srv/sample.json'
large_file = '/srv/2015-01-08_geo_en_it_10M.plain.json'

#one of two file above ^^^
input_file = large_file

threshold = 1000
sample_size = 0.1

def text_cleaner(text):
    text = re.sub('[^A-Za-z0-9@_]+|http.*|RT', ' ', text.lower())
    return text

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

#only for users feedback
def print_dict(mydict):
    boh = sorted(mydict, key=mydict.get, reverse=True)
    print "\n\tTOP 20 ABSOLUTE\n"
    count = 1
    for x in boh[0:20]:
       print str(count) + "\t" + str(x) + ": " + str(mydict[x])
       count +=1

    print "\n"

#order words with alphabet order (in order to avoid duplicates occurrences)
def alphabet_ordered(a, b, i):
        temp = []
        if i>0:
                for element in a:
                        temp.append(element)
        else:
                temp.append(a)
        temp.append(b)
        temp.sort()
        return tuple(temp)

def nested_loop_rank(tweet):
        resultlist = []
        #prendo il testo del tweet
        text = tweet #parse_text(tweet)
        res = text

        #trovo quanti nested loop devo fare
        rank = len(dict_list)
        i = 0
        while i<rank:
                actual_dict = dict_list[i]
                a = b = 0
                temp = []
                while a < len(res):
                        if res[a] in actual_dict:
                                while b < len(text):
                                        if text[b] in dict_list[0]:
                                                if text[b] not in res[a]:
                                                        temp.append(alphabet_ordered(res[a], text[b], i))
                                        b+=1
                        a+=1
                res = temp
                #passo al rank successivo
                i+=1
        return res

def nested_loop(tweet):
    resultlist = []
    text = parse_text(tweet)
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

#for all words of the dictionary, remove only those that have value lower than threshold
def filter_dict(my_dict):
    for key, value in my_dict.items():
        if value < threshold:
            del my_dict[key]

if __name__ == '__main__':
    print "\n### main.py ###\n"
    print "File: " + input_file
    print "Threshold: " + str(threshold)
    print "Sample size: " + str(sample_size)


    #create spark context
    sc = SparkContext(appName=name)

    stopwords = open('/home/e01/stopwords.txt','r').read().splitlines()
    #stopwords = sc.textFile('/home/e01/stopwords.txt').filter(lambda x: len(x)>2).collect()

    #read tweets file (sample or complete) and parse text from every tweet
#    tweets = sc.textFile(input_file).sample(False, sample_size, 42).flatMap(lambda x: parse_text(x)).countByValue()

    basket = sc.textFile(input_file).sample(False, sample_size, 42).map(lambda x: parse_text(x)).collect()
    print "Finita la lettura del basket...\n"
    print sc.parallelize(basket).take(3)
    tweets = sc.parallelize(basket).flatMap(lambda x: x).countByValue()

    print "Finito il conteggio delle parole...\n"
    #print tweets.take(4)
    filter_dict(tweets)
    print_dict(tweets)
    #tweets.filter(lambda x: tweets[x]>threshold)

    #print tweets

    #read file again, this time we will find every couples occurrences
#    bucket = sc.textFile(input_file).sample(False, sample_size, 42).flatMap(lambda x: nested_loop(x)).countByValue()

    #remove couples with occurrences lower than threshold
#    filter_dict(bucket)
    #print bucket
#    print_dict(bucket)

    dict_list = []
    dict_list.append(tweets)

    i=0
    while len(dict_list[i]) > 0:
        i += 1
        print "Inizio calcolo rank: " + str(i+1)

        #my_dict = sc.textFile(input_file).sample(False, sample_size,42).flatMap(lambda x: nested_loop_rank(x)).countByValue()
        my_dict = sc.parallelize(basket).flatMap(lambda x: nested_loop_rank(x)).countByValue()

        filter_dict(my_dict)

        print "lunghezza risultati: " + str(len(my_dict))
        dict_list.append(my_dict)

        if len(my_dict) > 0:
                print_dict(my_dict)
        else:
                print "Non ci sono piu' risultati utili"
        #print my_dict
