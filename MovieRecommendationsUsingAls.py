from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS, Rating
import sys

conf = SparkConf().setMaster("local[*]").setAppName("MovieRecommendationUsingAls")
sc = SparkContext(conf=conf)


def movieNames():
    movie_name = {}
    with open("C:/GitHub/PYSPARK/Pyspark/Data/movies.csv", encoding='utf-8') as F:
        for lines in F:
            movie_id = int(lines.split(',')[0])
            moviename = lines.split(',')[1]
            movie_name[movie_id] = moviename
    return movie_name


movie_name = movieNames()

movie_data = sc.textFile("C:/GitHub/PYSPARK/Pyspark/Data/ratings.csv")
mapped_data = movie_data.map(lambda x: Rating(int(x.split(',')[0]), int(x.split(',')[1]), float(x.split(',')[2]))).cache()

user_id = int(sys.argv[1])

model = ALS.train(mapped_data, 10, 5)

print("----------------USER MOVIES IN THE LIST-------------------")

user_data = mapped_data.filter(lambda x: x[0] == user_id)

for data in user_data.collect():
    print(movie_name[data[1]], " : ", data[2])

print("-----------------MOVIE  RECOMMENDATIONS FOR USER-------------------")

recommendations = model.recommendProducts(user_id, 10)

for recomedmovies in recommendations:
    print(movie_name[recomedmovies[1]], " AND SCORE", recomedmovies[2])
