from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, sum, count, avg, max, min, row_number, rank, dense_rank

spark = SparkSession.builder \
    .appName("Spark1 Application") \
    .getOrCreate()