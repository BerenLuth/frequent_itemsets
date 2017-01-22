from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark import SparkContext
import re

name = "RandomSampling"

small_file = '/srv/sample.json'
large_file = '/srv/2015-01-08_geo_en_it_10M.plain.json'

#one of two file above ^^^
input_file = large_file

threshold_percentage = 0.005
#threshold = 500
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
#prende in input una tupla, una stringa e la dimensione della tupla del primo parametro
#ritorna un'unica tupla di dimensione i contenente tutti gli elementi
def alphabet_ordered(a, b, i):
        temp = []
        #se i>0 allora a e' effettivamente una tupla e metto tutti i suoi elementi in una lista
        if i>0:
                for element in a:
                        temp.append(element)
        #altrimenti vuol dire che a e' una singola parola e metto solo quella nella lista
        else:
                temp.append(a)
        #metto anche b nella lista
        temp.append(b)
        #ordino
        temp.sort()
        #ritorno un'unica tupla in ordine alfabetico
        return tuple(temp)

#A partire da un testo di un tweet genera le tuple i-esime
#in base alla lunghezza di dict_list
def nested_loop_rank(text):
        resultlist = []

        #partiamo dalla semplice lista di parole
        #successivamente res conterra' tuple di due, tre o piu' elementi
        res = text

        #trovo quanti nested loop devo fare
        rank = len(dict_list)
        i = 0
        while i<rank:
                #prendo il dizionario relativo alla dimensione delle tuple che sto generando
                actual_dict = dict_list[i]
                a = b = 0
                #conterra' le tuple generate ad ogni iterazione
                temp = []
                #per ogni elemento di res (parole singole o tuple di dimensioni sempre maggiori)
                while a < len(res):
                        #se l'elemento e' contenuto nel dizionario relativo
                        if res[a] in actual_dict:
                                #scorro tutte le parole del tweet
                                #(in questo caso si tratta sempre di singole parole e non di tuple)
                                while b < len(text):
                                        #se la parola e' presente nel dizionario delle parole singole
                                        if text[b] in dict_list[0]:
                                                #se la parola non e' gia' contenuta nella tupla res[a]
                                                if text[b] not in res[a]:
                                                        #aggiungo la parola all'interno della lista dei risultati temporanei
                                                        #la aggiungo tramite una funzione che crea una tupla di dimensione i
                                                        temp.append(alphabet_ordered(res[a], text[b], i))
                                        b+=1
                        a+=1
                #metto la lista dei risultati temporanei dentro alla lista del risultato
                res = temp
                #passo al rank successivo
                i+=1
        #ritorno solo le ultime tuple create (quelle di dimensione maggiore)
        return res

#for all words of the dictionary, remove only those that have value lower than threshold
def filter_dict(my_dict):
    for key, value in my_dict.items():
        if value < threshold:
            del my_dict[key]

if __name__ == '__main__':
    print "\n### main.py ###\n"
    print "File: " + input_file
    print "Threshold (%): " + str(threshold)
    print "Sample size: " + str(sample_size)

    #create spark context
    sc = SparkContext(appName=name)
    stopwords = open('/home/e01/stopwords.txt','r').read().splitlines()
    #stopwords = sc.textFile('/home/e01/stopwords.txt').filter(lambda x: len(x)>2).collect()

    # OLD CODE #
    #read tweets file (sample or complete) and parse text from every tweet
    #tweets = sc.textFile(input_file).sample(False, sample_size, 42).flatMap(lambda x: parse_text(x)).countByValue()
    # END OLD CODE #

    baskets = sc.textFile(input_file).sample(False, sample_size, 42).map(lambda x: parse_text(x)).collect()
    print "Finita la lettura del basket...\n"

    #find threshold as a percentage of number of baskets
    threshold = threshold_percentage * len(baskets)
    print "Thresold calcolato in base al numero di basket: " + str(threshold)

    #genera il dizionario delle parole
    tweets = sc.parallelize(baskets).flatMap(lambda x: x).countByValue()
    print "Finito il conteggio delle parole...\n"

    #filtra il dizionario scartando le parole poco frequenti
    filter_dict(tweets)
    #stampo i primi 20 valori del dizionario come indicazione
    print_dict(tweets)

    # OLD CODE #
    #read file again, this time we will find every couples occurrences
    #bucket = sc.textFile(input_file).sample(False, sample_size, 42).flatMap(lambda x: nested_loop(x)).countByValue()

    #remove couples with occurrences lower than threshold
    #filter_dict(bucket)
    #print bucket
    #print_dict(bucket)
    # END OLD CODE #

    #lista dei dizionari delle occorrenze
    #l'indice+1 del dizionario indica la grandezza delle tuple contenute nel dizionario dell'indice stesso
    #es: indice 0 = dizionario delle singole parole, indice 1 = dizionario delle coppie, indice 2 = dizionario delle triple...
    dict_list = []
    #inizializzo la lista inserendo il dizionario delle singole parole
    dict_list.append(tweets)
    #vado avanti a creare dizionari (e a riempirli) fino a quando non trovero' un dizionario vuoto (nessuna tupla i-esima frequente)
    i=0
    while len(dict_list[i]) > 0:
        i += 1
        print "Inizio calcolo rank: " + str(i+1)

        #my_dict = sc.textFile(input_file).sample(False, sample_size,42).flatMap(lambda x: nested_loop_rank(x)).countByValue()
        #creo il dizionario i-esimo: nested_loop_rank sfrutta la lunghezza di dict_list per arrivare al grado giusto
        my_dict = sc.parallelize(baskets).flatMap(lambda x: nested_loop_rank(x)).countByValue()

        #rimuovo le tuple poco frequenti dal dizionario
        filter_dict(my_dict)

        #stampo il numero di risultati per il dizionario i-esimo
        print "lunghezza risultati: " + str(len(my_dict))

        #aggiungo il dizionario appena creato alla lista dei dizionari
        #se quest'ultimo sara' vuoto, invalidera' la condizione del ciclo fermandolo
        dict_list.append(my_dict)

        #stampo i primi 20 elementi del dizionario solo se sono presenti
        if len(my_dict) > 0:
                print_dict(my_dict)
        else:
                print "\nNon ci sono piu' risultati utili\n"
