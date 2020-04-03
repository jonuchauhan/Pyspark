from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("WordCountFlatmap")
sc = SparkContext(conf=conf)
input = sc.textFile("C:\spark\TEXT.txt")
words = input.flatMap(lambda x: x.split()).map(lambda x: (x, 1))
wordcount = words.reduceByKey(lambda x, y: (x+y)).map(lambda x: (x[1], x[0])).sortByKey(ascending= False)

for count, word in wordcount.collect():
    print(word, '=', count)
