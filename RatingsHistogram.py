from pyspark import SparkContext, SparkConf
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)
input = sc.textFile("C:/spark/ratings.txt")
ratings_split = input.map(lambda x: x.split(',')[2])
ratings = ratings_split.countByValue()
sorted_result = collections.OrderedDict(sorted(ratings.items()))
for ratings, occurance in sorted_result.items():
    print(ratings, occurance)
