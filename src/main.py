from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, sum, count, avg, max, min, row_number, rank, dense_rank
from pyspark.sql.functions import when

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Spark1 Application") \
    .getOrCreate()

# Load CSV data files
print("Loading data files...")
customers_df = spark.read.csv("data/customers.csv", header=True, inferSchema=True)
products_df = spark.read.csv("data/products.csv", header=True, inferSchema=True)
sales_df = spark.read.csv("data/sales.csv", header=True, inferSchema=True)

# Display the schema of the loaded data
print("Customers DataFrame Schema:")
customers_df.printSchema()
print("Products DataFrame Schema:")
products_df.printSchema()
print("Sales DataFrame Schema:")
sales_df.printSchema()

# Show the first few rows of each DataFrame
print("Customers DataFrame Sample:")
customers_df.show(5)
print("Products DataFrame Sample:")
products_df.show(5)
print("Sales DataFrame Sample:")
sales_df.show(5)

# Data cleaning

# Check for null values in customers DataFrame
print("Null values in Customers DataFrame:")
customers_nulls = customers_df.select([count(when(col(c).isNull(), c)).alias(c) for c in customers_df.columns])
customers_nulls.show()
# Check for null values in products DataFrame
print("Null values in Products DataFrame:")
products_nulls = products_df.select([count(when(col(c).isNull(), c)).alias(c) for c in products_df.columns])
products_nulls.show()
# Check for null values in sales DataFrame
print("Null values in Sales DataFrame:")
sales_nulls = sales_df.select([count(when(col(c).isNull(), c)).alias(c) for c in sales_df.columns])
sales_nulls.show()

