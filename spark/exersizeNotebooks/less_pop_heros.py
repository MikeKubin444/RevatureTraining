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

most_obscure = connections.sort(F.col("connections").asc())
# most_obscure.show()
num_most_obscure = most_obscure.first()["connections"]
most_obscure = most_obscure.filter(F.col("connections") == num_most_obscure)
# most_obscure.show()

# this prints each name as its own dataframe. Could be formated a bit better.
for i in range(most_obscure.count()):
    row = most_obscure.collect()[i]
    print(row)

    name = names.filter(F.col("id") == row["id"])
    name.show()