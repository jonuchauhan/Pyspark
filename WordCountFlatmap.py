from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("WordCountFlatmap")
sc = SparkContext(conf=conf)
input = sc.textFile("C:\spark\TEXT.txt")
words = input.flatMap(lambda x: x.split())
wordcount = words.countByValue()
print(wordcount)

for word, count in wordcount.items():
    print(word, count)



