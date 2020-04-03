from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("MinimumtemByStation")
sc = SparkContext(conf=conf)


def parseLines(lines):
    stationid = lines.split(',')[0]
    tempAtt = lines.split(',')[2]
    temp = float(lines.split(',')[3]) * 0.1 * (9 / 5) + 32
    return (stationid, tempAtt, temp)


input = sc.textFile("C:/spark/1800.csv")
parsed_map = input.map(parseLines)
MinTempBystation = parsed_map.filter(lambda x: 'TMIN' in x[1]).map(lambda x: (x[0], x[2])).reduceByKey(lambda x, y: min(x, y))
MaxTempBystation = parsed_map.filter(lambda x: 'TMAX' in x[1]).map(lambda x: (x[0], x[2])).reduceByKey(lambda x, y: max(x, y))
for a in MinTempBystation.collect():
    print(a)
for b in MaxTempBystation.collect():
    print(b)