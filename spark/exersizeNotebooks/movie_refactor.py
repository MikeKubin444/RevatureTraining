import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

from math import sqrt


# ----------------------------
# Compute cosine similarity
# ----------------------------
def computeCosineSimilarity(data):
    pairScores = data\
        .withColumn("xx", func.col("rating1") * func.col("rating1"))\
        .withColumn("yy", func.col("rating2") * func.col("rating2"))\
        .withColumn("xy", func.col("rating1") * func.col("rating2"))
    
    calcSim = pairScores\
        .groupBy("movie1", "movie2")\
        .agg(
            func.sum(func.col("xy")).alias("numerator"),
            (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"),
            func.count(func.col("xy")).alias("numPairs")
        )
    
    ret = calcSim.withColumn(
        "score", 
        func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")) \
            .otherwise(0)
        ).select("movie1", "movie2", "score", "numPairs")
    return ret


def getMovieName(movieNames, movieID):
    return movieNames.value.get(movieID, "unknown")


# # ----------------------------
# # Main Spark setup
# # ----------------------------

spark = SparkSession.builder.appName("movie refactor").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# # ----------------------------
# # S3 paths (CHANGE THIS)
# # ----------------------------
MOVIES_PATH = "s3a://my-big-bucket-311233338741-us-east-2-an/ml-1m/movies.dat"
RATINGS_PATH = "s3a://my-big-bucket-311233338741-us-east-2-an/ml-1m/ratings.dat"
OUTPUT_PATH = "s3a://my-big-bucket-311233338741-us-east-2-an/output2TheSequel/movie-sims"


# # ----------------------------
# # Load and broadcast movie names
# # --------------------------


# ----------------------------
# Load ratings from S3
# ----------------------------
print("Loading ratings from S3...")
movieRatingSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True)
])

movieNamesSchema = StructType([ \
                               StructField("movieID", IntegerType(), True), \
                               StructField("movieTitle", StringType(), True) \
])

movieNames=spark.read.option("sep", "::").schema(movieNamesSchema).csv(MOVIES_PATH)
movieRatings=spark.read.option("sep", "::").schema(movieRatingSchema).csv(RATINGS_PATH)
ratings = movieRatings.select("userID", "movieID", "rating")
movieNamesDict = dict(movieNames.select("movieID", "movieTitle").rdd.map(tuple).collect())
broadcastNames = spark.sparkContext.broadcast(movieNamesDict)

# # ----------------------------
# # Build movie pairs
# # ----------------------------
ratingsPartitioned = ratings.repartition(100, "userID")

moviePairs = (
    ratingsPartitioned.alias("r1")
    .join(
        ratingsPartitioned.alias("r2"),
        (func.col("r1.userID") == func.col("r2.userID")) &
        (func.col("r1.movieID") < func.col("r2.movieID"))
    )
    .select(
        func.col("r1.movieID").alias("movie1"),
        func.col("r2.movieID").alias("movie2"),
        func.col("r1.rating").alias("rating1"),
        func.col("r2.rating").alias("rating2")
    )
)

moviePairSimilarities = computeCosineSimilarity(moviePairs).cache()




# # Optional: save full results
moviePairSimilarities.write.mode("overwrite").parquet(OUTPUT_PATH)

# ----------------------------
# Query similar movies
# ----------------------------
if len(sys.argv) > 1:

    movieID = int(sys.argv[1])

    # update to .97 and 50
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50

    filteredResults = moviePairSimilarities.filter(
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
        (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold)
    )

    ret = filteredResults.orderBy(func.desc("score")).limit(10).collect()

    print("\nTop 10 similar movies for:", getMovieName(broadcastNames, movieID))

    for x in ret:
        similarMovieID = x.movie1
        if (similarMovieID == movieID):
            similarMovieID = x.movie2
        print(getMovieName(broadcastNames, similarMovieID), "score:", str(x.score), "strength:", str(x.numPairs))

spark.stop()