from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

# Create Spark Session
spark = SparkSession.builder \
    .appName("Customer_ETL_Pipeline") \
    .getOrCreate()

# Source File Path
source_path = r"C:\Users\shrir\OneDrive\Desktop\customer-etl-project\data\customer_data.txt"

# Read Source File
df = spark.read \
    .option("header", True) \
    .option("delimiter", "|") \
    .csv(source_path)

print("===== SOURCE DATA =====")
df.show()

# Transform Data
transformed_df = df.withColumn(
    "payment_status",
    upper(col("payment_status"))
)

# Filter Only Paid Customers
paid_df = transformed_df.filter(
    col("payment_status") == "PAID"
)

print("===== TRANSFORMED DATA =====")
paid_df.show()

# Destination Path
destination_path = r"D:\customer_output"

# Write Output
paid_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(destination_path)

print("ETL Pipeline Completed Successfully!")

# Stop Spark
spark.stop()