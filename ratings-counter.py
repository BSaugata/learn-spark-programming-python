from pyspark import SparkConf, SparkContext
import collections, time

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)
start_time = time.time()

lines = sc.textFile("data/input/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()


sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

print("--- %s seconds ---" % (time.time() - start_time))
