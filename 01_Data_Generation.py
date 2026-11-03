# Databricks notebook source

# Project: Sales Analytics & Profitability Analysis
# Step 1: Verify the Spark environment

print("Project: Sales Analytics & Profitability Analysis")
print("Spark Version:", spark.version)

display(spark.sql("SELECT current_catalog() AS catalog, current_schema() AS schema"))


# COMMAND ----------


import random
from datetime import date, timedelta
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, DateType
)

random.seed(42)

base_path = "/Volumes/workspace/default/sales_project_files/raw"

# 1. Create the product master: 50 records
categories = {
    "Food": ["Rice", "Pasta", "Cereal"],
    "Beverages": ["Juice", "Coffee", "Tea"],
    "Snacks": ["Chips", "Biscuits", "Nuts"],
    "Desserts": ["Cake", "Chocolate", "Ice Cream"],
    "Electronics": ["Headphones", "Keyboard", "Mouse"]
}

products = []

for i in range(1, 51):
    category = list(categories.keys())[(i - 1) % len(categories)]
    sub_category = categories[category][(i - 1) % len(categories[category])]
    price = round(random.uniform(100, 2000), 2)
    cost_price = round(price * random.uniform(0.50, 0.80), 2)

    products.append((
        f"P{i:03d}", f"{sub_category} {i}",
        category, sub_category, price, cost_price
    ))

product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("sub_category", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("cost_price", DoubleType(), False)
])

products_df = spark.createDataFrame(products, product_schema)

# 2. Create sales data: 1,000 records
sales = []
start_date = date(2025, 1, 1)
end_date = date(2026, 10, 28)
days_between = (end_date - start_date).days

locations = ["India", "USA", "UK", "Canada", "Australia"]
regions = ["North", "South", "East", "West"]
segments = ["Consumer", "Corporate", "Small Business"]
sources = ["Website", "Mobile App", "Store"]
statuses = ["Completed", "Completed", "Completed", "Cancelled", "Returned"]
discounts = [0.00, 0.05, 0.10, 0.15, 0.20]

for i in range(1, 1001):
    product = random.choice(products)
    order_date = start_date + timedelta(days=random.randint(0, days_between))

    sales.append((
        f"O{i:05d}",
        f"C{random.randint(1, 100):03d}",
        product[0],
        order_date,
        random.randint(1, 10),
        product[4],
        random.choice(discounts),
        random.choice(statuses),
        random.choice(locations),
        random.choice(regions),
        random.choice(segments),
        random.choice(sources)
    ))

sales_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("order_date", DateType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("discount", DoubleType(), False),
    StructField("order_status", StringType(), False),
    StructField("location", StringType(), False),
    StructField("region", StringType(), False),
    StructField("segment", StringType(), False),
    StructField("order_source", StringType(), False)
])

sales_df = spark.createDataFrame(sales, sales_schema)

# 3. Save both datasets as CSV
sales_path = f"{base_path}/sales_data"
products_path = f"{base_path}/product_master"

(sales_df.write
 .mode("overwrite")
 .option("header", True)
 .csv(sales_path))

(products_df.write
 .mode("overwrite")
 .option("header", True)
 .csv(products_path))

print("Dataset generation completed!")
print("Sales records:", sales_df.count())
print("Product records:", products_df.count())


# COMMAND ----------


# Verify that both datasets were saved to persistent storage

base_path = "/Volumes/workspace/default/sales_project_files/raw"

print("Sales data folder:")
display(dbutils.fs.ls(f"{base_path}/sales_data"))

print("Product master folder:")
display(dbutils.fs.ls(f"{base_path}/product_master"))


# COMMAND ----------


# Preview the generated sales records
display(sales_df.limit(10))

# Preview the product master
display(products_df.limit(10))
