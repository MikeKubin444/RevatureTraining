from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, udtf
from pyspark.sql.types import StringType
import re

spark = SparkSession.builder.appName('Hashtag Extractor').getOrCreate()
spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext


nums = sc.parallelize([1,2,3,4,5,6,7])

print(nums.collect())

data = [
    ("Learning AI with #ML",),
    ("Explor #DataScience",), 
    ("Not a hashtag in sight",)]

df = spark.createDataFrame(data, ['text'])

@udf(returnType=StringType())
def count_hashtag(text: str):
    if text:
        return len(re.findall(r"#\w+", text))
    

@udtf(returnType="hashtag: string")
class HashtagExtractor:
    def eval(self, text:str):
        if text:
            hashtags = re.findall(r"#\w+", text)
            for hashtag in hashtags:
                yield (hashtag,)


spark.udf.register("count_hashtag", count_hashtag)
spark.udtf.register("HashtagExtractor", HashtagExtractor)
# spark.sql("SELECT count_hashtag('Welcome to #ApacheSpark and #BigData') AS hashtag_count").show()


# df.selectExpr("text", "count_hashtag(text) AS num_hashtags").show()

# spark.sql("SELECT * FROM HashtagExtractor('welcome to #apachespark and #BigData!')").show()


df.createOrReplaceTempView("tweets")

spark.sql("SELECT text, hashtag FROM tweets, LATERAL HashtagExtractor(text)").show()


spark.stop()