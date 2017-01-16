from pyspark import SparkContext

sc = SparkContext()

textfile = sc.textFile('/srv/2015-01-08_geo_en_it_10M.plain.json')
print textfile.take(1)
