from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("local").setAppName("MostpopularMovie")
sc = SparkContext(conf=conf)


def parsedinput(lines):
    MovieID = int(lines.split(',')[1])
    Ratings = float(lines.split(',')[2])
    return (MovieID, Ratings)


input = sc.textFile("C:/GitHub/PYSPARK/Pyspark/Data/ratings.csv")

MaximumMovies = input.map(parsedinput).map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: (x + y)).map(
    lambda x: (x[1], x[0])).sortByKey(
    ascending=False)
MaximumratingByMovie = input.map(parsedinput).reduceByKey(lambda x, y: (x + y)).map(lambda x: (x[1], x[0])).\
    sortByKey(ascending=False)

for a in MaximumMovies.collect():
    print("MOVIEID", a[1], "=", a[0])

for b in MaximumratingByMovie.collect():
    print("MOVIEID", a[1], "=", a[0])
