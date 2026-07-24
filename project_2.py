# Import the SparkSession class, which is the entry point for working with Spark.
from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import *


# Create and configure a Spark session.
spark = (
    SparkSession.builder

    # Set a name for the Spark application (shows up in Spark UI/logs).
    .appName("Proj2")

    # Enable Apache Iceberg SQL extensions so Spark understands
    # Iceberg-specific SQL commands and table operations.
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # Register a catalog named "glue_catalog".
    # Spark will use this catalog whenever tables are referenced with
    # the prefix "glue_catalog".
    .config(
        "spark.sql.catalog.glue_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
    )

    # Tell Spark that this catalog should use AWS Glue
    # as the metadata store for Iceberg tables.
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog"
    )

    # Specify the S3 warehouse location where Iceberg table data
    # and metadata files will be stored.
    .config(
        "spark.sql.catalog.glue_catalog.warehouse",
        "s3://s3://<YOUR_BUCKET_NAME>/warehouse//warehouse/"#fix this
    )

    # Configure Iceberg to use the S3FileIO implementation
    # for reading and writing data in Amazon S3.
    .config(
        "spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )

    # Create the Spark session with all of the above settings.
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read the Titanic dataset from a Parquet file stored in S3
# and load it into a Spark DataFrame.

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("order_date", DateType()),
    StructField("ship_date", DateType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", StringType()),
    StructField("discount_pct", StringType()),
    StructField("total_amount", FloatType()),
    StructField("payment_method", StringType()),
    StructField("order_status", StringType())
])

product_schema = StructType([
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("brand", StringType()),
    StructField("price", StringType()),
    StructField("cost", FloatType()),
    StructField("stock_quantity", IntegerType()),
    StructField("weight_kg", FloatType()),
    StructField("created_date", DateType()),
    StructField("is_active", StringType())
])

customers_schema = StructType([
    StructField("customer_id", IntegerType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("signup_date", DateType()),
    StructField("country", StringType()),
    StructField("state", StringType()),
    StructField("postal_code", StringType()),
    StructField("is_active", BooleanType()),
    StructField("loyalty_points", IntegerType())

])


orders_df = spark.read.option("header", True).schema(order_schema).csv("s3://<bucket>/orders.csv")
product_df = spark.read.option("header", True).schema(product_schema).csv("s3://<bucket>/products.csv")
customers_df = spark.read.option("header", True).schema(customers_schema).csv("s3://<bucket>/customers.csv")


# Display the DataFrame's schema (column names and data types)
# to verify the data was loaded correctly.
orders_df.printSchema()
product_df.printSchema()
customers_df.printSchema()

customers_df_clean = customers_df.drop_duplicates(["customer_id"])
# Regex pattern for a standard email
customers_df_clean = customers_df_clean.withColumn("first_name", func.trim(func.col("first_name")))
customers_df_clean = customers_df_clean.withColumn("last_name", func.trim(func.col("last_name")))
customers_df_clean = customers_df_clean.withColumn("email", func.trim(func.col("email")))
customers_df_clean = customers_df_clean.withColumn("phone", func.trim(func.col("phone")))
customers_df_clean = customers_df_clean.withColumn("country", func.trim(func.col("country")))
customers_df_clean = customers_df_clean.withColumn("state", func.trim(func.col("state")))
customers_df_clean = customers_df_clean.withColumn("postal_code", func.trim(func.col("postal_code")))
email_pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"# Keep only strings that match the email pattern, otherwise return null
customers_df_clean = customers_df_clean.withColumn(
    "email",
    func.when(
        func.col("email").rlike(email_pattern),
        func.col("email")
    )
)
phone_pattern = (
    r"^(\+?\d{1,3}[\s.-]?)?"
    r"\(?\d{3}\)?[\s.-]?"
    r"\d{3}[\s.-]?"
    r"\d{4}$"
)
# Keep only strings that match the phone pattern, otherwise return null
customers_df_clean = customers_df_clean.withColumn(
    "phone",
    func.when(
        func.col("phone").rlike(phone_pattern),
        func.col("phone")
    )
)
customers_df_clean = customers_df_clean.withColumn(
    "phone",
    func.regexp_replace("phone", r"[^\d+]", "")
)
customers_df_clean = customers_df_clean.withColumn(
    "phone",
    func.when(
        func.col("phone").startswith("+"),
        func.col("phone")
    ).otherwise(
        func.concat(func.lit("+1"), func.col("phone"))
    )
)
customers_df_clean = customers_df_clean.dropna()
customers_df_clean = customers_df_clean.withColumn("loyalty_points",func.greatest(func.col("loyalty_points"), func.lit(0)))


orders_df_clean = orders_df.dropDuplicates(["order_id"])

orders_df_clean = (
    orders_df_clean
    .withColumn("customer_id", func.trim("customer_id"))
    .withColumn("product_id", func.trim("product_id"))
    .withColumn("payment_method", func.trim("payment_method"))
    .withColumn("order_status", func.trim("order_status"))
)

orders_df_clean = orders_df_clean.withColumn(
    "unit_price",
    func.regexp_replace("unit_price", r"[$,]", "").try_cast("decimal")
)

orders_df_clean = orders_df_clean.withColumn(
    "discount_pct",
    func.when(
        func.col("discount_pct").rlike(r"^\d+(\.\d+)?$"),
        func.col("discount_pct").try_cast("float")
    )
)

orders_df_clean = orders_df_clean.withColumn(
    "quantity",
    func.greatest(func.col("quantity"), func.lit(0))
)

orders_df_clean = orders_df_clean.withColumn(
    "discount_pct",
    func.when(func.col("discount_pct") > 100, 100)
        .when(func.col("discount_pct") < 0, 0)
        .otherwise(func.col("discount_pct"))
)

orders_df_clean = orders_df_clean.dropna(
    subset=[
        "customer_id",
        "product_id",
        "unit_price",
        "discount_pct"
    ]
)

product_df_clean = product_df.dropDuplicates(["product_id"])

product_df_clean = (
    product_df_clean
    .withColumn("product_name", func.trim("product_name"))
    .withColumn("category", func.trim("category"))
    .withColumn("brand", func.trim("brand"))
)

product_df_clean = product_df_clean.withColumn(
    "price",
    func.regexp_replace("price", r"[$,]", "").try_cast("decimal")
)

product_df_clean = product_df_clean.withColumn(
    "weight_kg",
    func.col("weight_kg").try_cast("float")
)

product_df_clean = product_df_clean.withColumn(
    "price",
    func.greatest(func.col("price"), func.lit(0.0))
)

product_df_clean = product_df_clean.withColumn(
    "stock_quantity",
    func.greatest(func.col("stock_quantity"), func.lit(0))
)

product_df_clean = product_df_clean.withColumn(
    "is_active",
    func.when(
        func.lower(func.col("is_active")).isin("true", "yes", "y"),
        True
    ).otherwise(False)
)

product_df_clean = product_df_clean.fillna({
    "weight_kg": 0.1
})

product_df_clean = product_df_clean.dropna(
    subset=["price"]
)

# Create an Iceberg database (namespace) in AWS Glue if it
# doesn't already exist.
spark.sql("""
CREATE DATABASE IF NOT EXISTS glue_catalog.iceberg_catalog_db
""")






# Write the DataFrame as an Iceberg table.
(
    customers_df_clean.writeTo(
        # Fully qualified table name:
        # catalog.database.table
        "glue_catalog.iceberg_catalog_db.customers"
    )
    # Specify that the table format should be Apache Iceberg.
    .using("iceberg")

    # Create the table if it doesn't exist.
    # If it already exists, replace it with the new data.
    .createOrReplace()
)

(
    orders_df_clean.writeTo(
        # Fully qualified table name:
        # catalog.database.table
        "glue_catalog.iceberg_catalog_db.orders"
    )
    # Specify that the table format should be Apache Iceberg.
    .using("iceberg")

    # Create the table if it doesn't exist.
    # If it already exists, replace it with the new data.
    .createOrReplace()
)

(
    product_df_clean.writeTo(
        # Fully qualified table name:
        # catalog.database.table
        "glue_catalog.iceberg_catalog_db.products"
    )
    # Specify that the table format should be Apache Iceberg.
    .using("iceberg")

    # Create the table if it doesn't exist.
    # If it already exists, replace it with the new data.
    .createOrReplace()
)


# Query the newly created Iceberg table to verify that the
# data was written successfully.
spark.sql("""
SELECT COUNT(*)
FROM glue_catalog.iceberg_catalog_db.customers
""").show()

spark.sql("""
SELECT COUNT(*)
FROM glue_catalog.iceberg_catalog_db.orders
""").show()

spark.sql("""
SELECT COUNT(*)
FROM glue_catalog.iceberg_catalog_db.products
""").show()


# Stop the Spark session and release cluster resources.
spark.stop()