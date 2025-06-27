from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, sum, count, avg, max, min, row_number, rank, dense_rank

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Simple Spark Data Explorer") \
    .getOrCreate()

# Function to load CSV data files
def load_data():
    print("Loading data files...")
    customers = spark.read.csv("data/customers.csv", header=True, inferSchema=True)
    products = spark.read.csv("data/products.csv", header=True, inferSchema=True)
    sales = spark.read.csv("data/sales.csv", header=True, inferSchema=True)
    return customers, products, sales

# Function to show DataFrame schema and sample data
def show_schema_and_samples(df, name):
    print(f"{name} DataFrame Schema:")
    df.printSchema()
    print(f"{name} DataFrame Sample:")
    df.show(5)

# Function to display the main menu
def main_menu():
    print("\nSimple Spark Data Explorer")
    print("1. Show Customers")
    print("2. Show Products")
    print("3. Show Sales")
    print("4. Exit")
    choice = input("Select an option: ")
    return choice

# Main function to run the application
def main():
    customers_df, products_df, sales_df = load_data()
    while True:
        choice = main_menu()
        if choice == "1":
            show_schema_and_samples(customers_df, "Customers")
        elif choice == "2":
            show_schema_and_samples(products_df, "Products")
        elif choice == "3":
            show_schema_and_samples(sales_df, "Sales")
        elif choice == "4":
            print("Exiting...")
            break
        else:
            print("Invalid option. Please try again.")

# Entry point for the application
if __name__ == "__main__":
    main()

