from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, IntegerType, StructType

spark = SparkSession.builder.appName("Supes").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType())
])


names = spark.read.schema(schema).option("sep", " ").csv("../data/MarvelNames.txt")
lines = spark.read.text("../data/MarvelGraph.txt")

connections = lines.withColumn("id", F.split(F.col("value"), " ")[0])\
    .withColumn("connections", F.size(F.split(F.col("value"), " ")) - 1)\
    .groupBy("id").agg(F.sum("connections").alias("connections"))

# connections.show()

most_pop = connections.sort(F.col("connections").desc()).first()
most_pop_name = names.filter(F.col("id") == most_pop[0]).select("name").first()

print("\n\n" + most_pop_name[0] + " is the most popular hero, with " + str(most_pop[1]) + " appearances!")


spark.stop()