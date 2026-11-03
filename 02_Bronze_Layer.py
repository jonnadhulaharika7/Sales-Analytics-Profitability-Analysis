# Databricks notebook source

# Project: Sales Analytics & Profitability Analysis
# Bronze Layer: Load raw CSV files and save as Delta tables

base_path = "/Volumes/workspace/default/sales_project_files/raw"

sales_path = f"{base_path}/sales_data"
products_path = f"{base_path}/product_master"

# Read raw sales CSV files
raw_sales_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(sales_path)
)

# Read raw product CSV files
raw_products_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(products_path)
)

# Save as project-specific Bronze Delta tables
sales_table = "workspace.bronze.sales_project_raw_sales"
products_table = "workspace.bronze.sales_project_raw_products"

(raw_sales_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(sales_table))

(raw_products_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(products_table))

print("Bronze ingestion completed!")
print("Sales records:", spark.table(sales_table).count())
print("Product records:", spark.table(products_table).count())
