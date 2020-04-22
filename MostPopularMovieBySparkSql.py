from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "C:/temp").appName(
    "MostPopularMovieBySparkSql").getOrCreate()


def movienames():
    movie_names_dict = {}
    with open("C:/GitHub/PYSPARK/Pyspark/Data/movies.csv", encoding='utf-8') as F:
        for lines in F:
            movie_id = int(lines.split(',')[0])
            movie_names = lines.split(',')[1]
            movie_names_dict[movie_id] = movie_names

    return movie_names_dict


movie_names = movienames()



input_movie_data = spark.sparkContext.textFile("C:/GitHub/PYSPARK/Pyspark/Data/ratings.csv")
mapped_input_data = input_movie_data.map(lambda x: Row(MOVIE_NAME=movie_names[int(x.split(',')[1])]))
Movie = spark.createDataFrame(mapped_input_data).createOrReplaceTempView("MOVIES")
FAMOUS_MOVIES = spark.sql("select MOVIE_NAME,count(*) as COUNTS  from MOVIES GROUP BY MOVIE_NAME ORDER BY COUNTS DESC LIMIT 100")

FAMOUS_MOVIES.show()
