from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, sum, count, avg, max, min, row_number, rank, dense_rank

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

