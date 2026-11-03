# Databricks notebook source

# Inspect Bronze table schemas

sales_table = "workspace.bronze.sales_project_raw_sales"
products_table = "workspace.bronze.sales_project_raw_products"

print("SALES TABLE SCHEMA")
spark.table(sales_table).printSchema()

print("PRODUCT TABLE SCHEMA")
spark.table(products_table).printSchema()

print("SALES SAMPLE")
display(spark.table(sales_table).limit(5))

print("PRODUCT SAMPLE")
display(spark.table(products_table).limit(5))


# COMMAND ----------

from pyspark.sql import functions as F

sales_df = spark.table("workspace.bronze.sales_project_raw_sales")
products_df = spark.table("workspace.bronze.sales_project_raw_products")

print("Sales record count:", sales_df.count())
print("Product record count:", products_df.count())

print("Duplicate sales rows:",
      sales_df.count() - sales_df.dropDuplicates().count())

print("Duplicate product rows:",
      products_df.count() - products_df.dropDuplicates().count())

print("Null counts in sales:")
display(sales_df.select([
    F.count(F.when(F.col(c).isNull(), 1)).alias(c)
    for c in sales_df.columns
]))

print("Null counts in products:")
display(products_df.select([
    F.count(F.when(F.col(c).isNull(), 1)).alias(c)
    for c in products_df.columns
]))

print("Order status distribution:")
display(sales_df.groupBy("order_status").count())

print("Invalid sales records:")
display(sales_df.filter(
    (F.col("quantity") <= 0) |
    (F.col("unit_price") < 0) |
    (F.col("discount") < 0) |
    (F.col("discount") > 1)
))

# COMMAND ----------

from pyspark.sql import functions as F

# Read Bronze tables
sales_df = spark.table("workspace.bronze.sales_project_raw_sales")
products_df = spark.table("workspace.bronze.sales_project_raw_products")

# Clean sales data
clean_sales_df = (
    sales_df
    .withColumn("order_id", F.trim(F.col("order_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("product_id", F.trim(F.col("product_id")))
    .withColumn("order_status", F.initcap(F.trim(F.col("order_status"))))
    .withColumn("location", F.trim(F.col("location")))
    .withColumn("region", F.initcap(F.trim(F.col("region"))))
    .withColumn("segment", F.trim(F.col("segment")))
    .withColumn("order_source", F.trim(F.col("order_source")))
)

# Clean product data
clean_products_df = (
    products_df
    .withColumn("product_id", F.trim(F.col("product_id")))
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("category", F.initcap(F.trim(F.col("category"))))
    .withColumn("sub_category", F.initcap(F.trim(F.col("sub_category"))))
)

# Save project-specific Silver tables
clean_sales_df.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.silver.sales_project_clean_sales"
)

clean_products_df.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.silver.sales_project_clean_products"
)

print("Silver cleaning completed successfully!")
print("Clean sales count:", clean_sales_df.count())
print("Clean products count:", clean_products_df.count())

# COMMAND ----------

from pyspark.sql import functions as F

# Read cleaned Silver tables
sales_df = spark.table("workspace.silver.sales_project_clean_sales")
products_df = spark.table("workspace.silver.sales_project_clean_products")

# Join sales and product data
enriched_sales_df = sales_df.join(
    products_df.select("product_id", "product_name", "category",
                       "sub_category", "cost_price"),
    on="product_id",
    how="left"
)

# Calculate financial metrics
enriched_sales_df = (
    enriched_sales_df
    .withColumn(
        "net_sales",
        F.round(
            F.col("quantity") * F.col("unit_price") *
            (1 - F.col("discount")), 2
        )
    )
    .withColumn(
        "product_cost",
        F.round(F.col("quantity") * F.col("cost_price"), 2)
    )
    .withColumn(
        "profit",
        F.round(F.col("net_sales") - F.col("product_cost"), 2)
    )
    .withColumn(
        "profit_margin_pct",
        F.when(
            F.col("net_sales") != 0,
            F.round(
                (F.col("profit") / F.col("net_sales")) * 100, 2
            )
        ).otherwise(0)
    )
)

# Save enriched Silver table
enriched_sales_df.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.silver.sales_project_enriched_sales"
)

print("Enriched sales count:", enriched_sales_df.count())

display(
    enriched_sales_df.select(
        "order_id", "product_id", "product_name",
        "category", "quantity", "unit_price", "discount",
        "net_sales", "product_cost", "profit",
        "profit_margin_pct", "order_status"
    ).limit(10)
)

# COMMAND ----------

from pyspark.sql import functions as F

enriched_df = spark.table(
    "workspace.silver.sales_project_enriched_sales"
)

print("Total records:", enriched_df.count())

print(
    "Records without product details:",
    enriched_df.filter(F.col("product_name").isNull()).count()
)

print(
    "Records with null financial values:",
    enriched_df.filter(
        F.col("net_sales").isNull() |
        F.col("product_cost").isNull() |
        F.col("profit").isNull() |
        F.col("profit_margin_pct").isNull()
    ).count()
)

print("Financial summary:")
display(
    enriched_df.filter(
        F.col("order_status") == "Completed"
    ).agg(
        F.round(F.sum("net_sales"), 2).alias("total_revenue"),
        F.round(F.sum("product_cost"), 2).alias("total_cost"),
        F.round(F.sum("profit"), 2).alias("total_profit"),
        F.countDistinct("order_id").alias("total_orders")
    )
)