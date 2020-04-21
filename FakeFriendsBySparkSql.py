from pyspark.sql import SparkSession
from pyspark.sql import Row


spark = SparkSession.builder.config("spark.sql.warehouse.dir","C:/temp").appName("FakeFriendsSparkSql").getOrCreate()

def convertRow(lines):
    feilds = lines.split(',')
    ID = int(feilds[0])
    Name = feilds[1]
    Age = int(feilds[2])
    NoOfFriends = int(feilds[3])
    return Row(ID=ID, NAME=Name, AGE=Age, NUMOFFRIENDS= NoOfFriends)


input = spark.sparkContext.textFile("C:/GitHub/PYSPARK/Pyspark/Data/fakefriends.csv")
mappedInput = input.map(convertRow)

People = spark.createDataFrame(mappedInput).createOrReplaceTempView("People")


AllTeenagers = spark.sql("select AGE,count(*) from People where AGE between 13 and 19 group by AGE order by AGE")

AllTeenagers.printSchema()

for teen in AllTeenagers.collect():
    print(teen)

