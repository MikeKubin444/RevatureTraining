from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, FloatType, LongType

from pyspark.sql.functions import udf

def load_movie_names() -> dict[int,str]:
    movie_names = {}
    with open("../ml-100k/u.item", "r", encoding="ISO-8859-1", errors="ignore") as file:
        for line in file:
            fields = line.split("|")
            movie_names[int(fields[0])] = fields[1]
        return movie_names
    
spark = SparkSession.builder.appName("Pop Movies").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


nameDict = spark.sparkContext.broadcast(load_movie_names())

schema = StructType([
    StructField("userID", IntegerType()),
    StructField("movieId", IntegerType()),
    StructField("rating", IntegerType()),
    StructField("timestamp", LongType())
])

moviesDF = spark.read.option("sep", "\t").schema(schema).csv("../ml-100k/u.data")
movie_rating_counts = moviesDF.groupBy("movieId").count()


@udf
def look_up_name(movieId: int) -> str:
    return nameDict.value.get(movieId, "Unknown")

movie_counts_with_names = movie_rating_counts.withColumn("movieTitle", look_up_name(F.col("movieId")))
sorted_movie_counts_with_names = movie_counts_with_names.orderBy(F.desc("count"))

sorted_movie_counts_with_names.show(10, False)