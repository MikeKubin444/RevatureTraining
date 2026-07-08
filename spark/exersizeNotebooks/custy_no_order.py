from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, IntegerType, StructType, LongType


spark = SparkSession.builder.appName("custyNoOrder").getOrCreate()
spark.sparkContext.setLogLevel("error")

custy_schema = StructType([
    StructField("customer_id", IntegerType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("city", StringType())
])

order_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("order_date", LongType()),
    StructField("amount", IntegerType())
])



custys = spark.read.schema(custy_schema).option("header", True).csv("../data/customers.csv")
orders = spark.read.schema(order_schema).option("header", True).csv("../data/orders.csv")

unique_custys = orders.select("customer_id").distinct()

custys_no_orders = custys.join(
    unique_custys,
    "customer_id",
    "left_anti"
)

custys_no_orders.show()

