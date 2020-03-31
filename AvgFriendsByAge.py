from pyspark import SparkContext, SparkConf


def parseinput(lines):
    field1 = lines.split(',')
    field2 = lines.split(',')
    age = int(field1[2])
    nooffriends = int(field2[3])
    return (age, nooffriends)


conf = SparkConf().setMaster("local").setAppName('AvgFriendsByAge')
sc = SparkContext(conf=conf)
input = sc.textFile("C:/spark/fakefriends.csv")
rd = input.map(parseinput)
Total = rd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).sortByKey()
AvgAge = Total.mapValues(lambda x: (x[0] / x[1])).sortByKey()
for k, v in Total.collect():
    """one of finding Avg without mapValue """
    print('AGE=', k, 'Avg=', v[0] / v[1])

for k, v in AvgAge.collect():
    print('AGE=', k, 'AVG=', v)
