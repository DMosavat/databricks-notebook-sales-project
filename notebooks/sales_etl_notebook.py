# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum, desc, count, avg
from delta import configure_spark_with_delta_pip

# COMMAND ----------

builder = (
    SparkSession.builder
    .appName("Notebook Sales ETL")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# COMMAND ----------

# Input file paths
customers_path = "data/customers.csv"
orders_path = "data/orders.csv"
order_items_path = "data/order_items.csv"
products_path = "data/products.csv"

# Output paths
sales_per_customer_delta_path = "output/sales_per_customer_delta"
sales_per_country_delta_path = "output/sales_per_country_delta"
product_sales_delta_path = "output/product_sales_delta"
order_summary_delta_path = "output/order_summary_delta"

# COMMAND ----------

# Read source CSV files
customers_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(customers_path)
)

orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(orders_path)
)

order_items_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(order_items_path)
)

products_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(products_path)
)

# COMMAND ----------

# Clean date columns
customers_df = customers_df.withColumn(
    "signup_date",
    to_date(col("signup_date"))
)

orders_df = orders_df.withColumn(
    "order_date",
    to_date(col("order_date"))
)

# COMMAND ----------

# Keep only completed orders
completed_orders_df = orders_df.filter(col("status") == "completed")

# COMMAND ----------

# Add line total for each order item
order_items_with_total_df = order_items_df.withColumn(
    "line_total",
    col("quantity") * col("unit_price")
)

# COMMAND ----------

# Build order totals
order_totals_df = (
    order_items_with_total_df
    .groupBy("order_id")
    .agg(sum("line_total").alias("order_total"))
)

# COMMAND ----------

# Build sales dataset
sales_df = (
    completed_orders_df
    .join(order_totals_df, on="order_id", how="inner")
    .join(customers_df, on="customer_id", how="inner")
)

# COMMAND ----------

# KPI: sales per customer
sales_per_customer_df = (
    sales_df
    .groupBy("customer_id", "name", "country")
    .agg(sum("order_total").alias("total_sales"))
    .orderBy(desc("total_sales"))
)

# COMMAND ----------

# KPI: sales per country
sales_per_country_df = (
    sales_df
    .groupBy("country")
    .agg(sum("order_total").alias("total_sales"))
    .orderBy(desc("total_sales"))
)

# COMMAND ----------

# KPI: product sales
product_sales_df = (
    completed_orders_df
    .join(order_items_with_total_df, on="order_id", how="inner")
    .join(products_df, on="product_id", how="inner")
    .groupBy("product_id", "product_name", "category")
    .agg(
        sum("quantity").alias("total_quantity"),
        sum("line_total").alias("total_sales")
    )
    .orderBy(desc("total_quantity"))
)

# COMMAND ----------

# KPI: order summary
order_summary_df = (
    sales_df
    .agg(
        count("order_id").alias("completed_order_count"),
        avg("order_total").alias("average_order_value")
    )
)

# COMMAND ----------

# Preview outputs
print("=== SALES PER CUSTOMER ===")
sales_per_customer_df.show()

print("=== SALES PER COUNTRY ===")
sales_per_country_df.show()

print("=== PRODUCT SALES ===")
product_sales_df.show()

print("=== ORDER SUMMARY ===")
order_summary_df.show()

# COMMAND ----------

# Write Delta outputs
sales_per_customer_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(sales_per_customer_delta_path)

sales_per_country_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(sales_per_country_delta_path)

product_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(product_sales_delta_path)

order_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(order_summary_delta_path)

# COMMAND ----------

# Read one Delta output back for verification
check_df = spark.read.format("delta").load(sales_per_customer_delta_path)
check_df.show()

# COMMAND ----------

spark.stop()