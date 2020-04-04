from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalAmountByCustomer")
sc = SparkContext(conf=conf)


def parsedinput(lines):
    customer_id = int(lines.split(',')[0])
    Amount = float(lines.split(',')[2])
    return (customer_id, Amount)


input = sc.textFile("C:/spark/customer-orders.csv")
mappedinput = input.map(parsedinput)
AmountByCustomer = mappedinput.reduceByKey(lambda x, y: (x + y)).map(lambda x : (x[1],x[0])).sortByKey(ascending=False)

for a in AmountByCustomer.collect():
    print(a[0], '=', a[1])
