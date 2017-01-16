# Password for ssh user lWMMC7uUGUHd

from pyspark import SparkContext
from pyspark.sql import SparkSession
import re

small_file = '/srv/sample.json'
large_file = '/srv/2015-01-08_geo_en_it_10M.plain.json'

if __name__ == '__main__':
    print "\n### main.py ###\n"

    spark_session = SparkSession.builder.appName("PythonSON").getOrCreate()

    tweets = spark_session.read.json(small_file)
    print tweets[1]


    print "\n"
