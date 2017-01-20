from pyspark import SparkContext

sc = SparkContext()

textfile = sc.textFile('/srv/sample.json')
print textfile.take(1)
