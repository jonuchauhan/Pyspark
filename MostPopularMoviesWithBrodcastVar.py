from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("MostPopularMoviesWithBrodcastVar")
sc = SparkContext(conf=conf)


def movienames():
    moviename = {}
    with open("C:/GitHub/PYSPARK/Pyspark/Data/movies.csv", encoding='utf-8') as f:
        for lines in f:
            movie_id = int(lines.split(',')[0])
            movie_names = lines.split(',')[1]
            moviename[movie_id] = movie_names
    return moviename


def rating(lines):
    MovieID = int(lines.split(',')[1])
    Ratings = float(lines.split(',')[2])
    return (MovieID, Ratings)


rating_input = sc.textFile("C:/GitHub/PYSPARK/Pyspark/Data/ratings.csv")

namedict = sc.broadcast(movienames())

mostmovies = rating_input.map(rating).map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: (x + y)). \
    map(lambda x: (x[1], namedict.value[x[0]])).sortByKey(ascending=False)

for a in mostmovies.collect():
    print(a[1], "=", a[0])

