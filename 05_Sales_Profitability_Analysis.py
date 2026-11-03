# Databricks notebook source
from pyspark.sql import functions as F

# Load Gold KPI tables
monthly_df = spark.table(
    "workspace.gold.sales_project_monthly_kpis"
)

product_df = spark.table(
    "workspace.gold.sales_project_product_kpis"
)

category_df = spark.table(
    "workspace.gold.sales_project_category_kpis"
)

customer_df = spark.table(
    "workspace.gold.sales_project_customer_kpis"
)

region_df = spark.table(
    "workspace.gold.sales_project_region_kpis"
)

source_df = spark.table(
    "workspace.gold.sales_project_source_kpis"
)

print("All Gold KPI tables loaded successfully!")

# COMMAND ----------

# Top 10 products by revenue

top_10_products = (
    product_df
    .select(
        "product_id",
        "product_name",
        "category",
        "total_quantity_sold",
        "total_revenue",
        "total_profit"
    )
    .orderBy(F.desc("total_revenue"))
    .limit(10)
)

display(top_10_products)

# COMMAND ----------

# Top 10 products by profit

top_10_profitable_products = (
    product_df
    .select(
        "product_id",
        "product_name",
        "category",
        "total_revenue",
        "total_cost",
        "total_profit",
        "profit_margin_pct"
    )
    .orderBy(F.desc("total_profit"))
    .limit(10)
)

display(top_10_profitable_products)

# COMMAND ----------

# Monthly revenue and profit analysis

monthly_trends = (
    monthly_df
    .select(
        "year_month",
        "total_orders",
        "total_revenue",
        "total_cost",
        "total_profit",
        "profit_margin_pct"
    )
    .orderBy("year_month")
)

display(monthly_trends)

# COMMAND ----------

monthly_revenue = (
    monthly_df
    .select(
        "month",
        "total_revenue"
    )
    .orderBy("month")
)

display(monthly_revenue)

# COMMAND ----------

# Top 10 highest-spending customers

top_customers = (
    customer_df
    .select(
        "customer_id",
        "segment",
        "location",
        "total_orders",
        "total_spending",
        "total_profit",
        "average_order_value"
    )
    .orderBy(F.desc("total_spending"))
    .limit(10)
)

display(top_customers)

# COMMAND ----------

display(top_customers)

# COMMAND ----------

category_performance = (
    category_df
    .select(
        "category",
        "total_orders",
        "total_quantity_sold",
        "total_revenue",
        "total_cost",
        "total_profit",
        "profit_margin_pct"
    )
    .orderBy(F.desc("total_profit"))
)

display(category_performance)

# COMMAND ----------

display(category_performance)


# COMMAND ----------

region_performance = (
    region_df
    .select(
        "region",
        "location",
        "total_orders",
        "total_quantity_sold",
        "total_revenue",
        "total_cost",
        "total_profit",
        "profit_margin_pct"
    )
    .orderBy(F.desc("total_revenue"))
)

display(region_performance)

# COMMAND ----------

display(region_performance)

# COMMAND ----------

source_performance = (
    source_df
    .select(
        "order_source",
        "total_orders",
        "total_quantity_sold",
        "total_revenue",
        "total_cost",
        "total_profit",
        "profit_margin_pct"
    )
    .orderBy(F.desc("total_revenue"))
)

display(source_performance)

# COMMAND ----------

display(source_performance)

# COMMAND ----------

overall_performance = (
    spark.table("workspace.silver.sales_project_enriched_sales")
    .filter(F.col("order_status") == "Completed")
    .agg(
        F.countDistinct("order_id").alias("total_completed_orders"),
        F.sum("quantity").alias("total_quantity_sold"),
        F.round(F.sum("net_sales"), 2).alias("total_revenue"),
        F.round(F.sum("product_cost"), 2).alias("total_cost"),
        F.round(F.sum("profit"), 2).alias("total_profit")
    )
    .withColumn(
        "profit_margin_pct",
        F.when(
            F.col("total_revenue") != 0,
            F.round(
                F.col("total_profit") / F.col("total_revenue") * 100, 2
            )
        ).otherwise(0)
    )
)

display(overall_performance)

# COMMAND ----------

display(overall_performance)

# COMMAND ----------

monthly_revenue = (
    spark.table("workspace.gold.sales_project_monthly_kpis")
    .select(
        "year_month",
        "total_revenue"
    )
    .orderBy("year_month")
)

display(monthly_revenue)

# COMMAND ----------

category_profit_chart = (
    category_df
    .select(
        "category",
        "total_revenue",
        "total_profit"
    )
    .orderBy(F.desc("total_profit"))
)

display(category_profit_chart)

# COMMAND ----------

category_chart_data = (
    category_df
    .select(
        "category",
        "total_revenue",
        "total_profit"
    )
    .orderBy("category")
)

display(category_chart_data)

# COMMAND ----------

category_revenue_chart = (
    category_df
    .select("category", "total_revenue")
    .orderBy(F.desc("total_revenue"))
)

display(category_revenue_chart)

# COMMAND ----------

from pyspark.sql import functions as F

category_revenue_chart = (
    category_df
    .groupBy("category")
    .agg(
        F.sum("total_revenue").alias("revenue")
    )
    .orderBy(F.desc("revenue"))
)

display(category_revenue_chart)

# COMMAND ----------

category_profit_chart = (
    category_df
    .select("category", "total_profit")
    .orderBy(F.desc("total_profit"))
)

display(category_profit_chart)

# COMMAND ----------

source_revenue_chart = (
    source_df
    .select(
        "order_source",
        "total_revenue"
    )
    .orderBy(F.desc("total_revenue"))
)

display(source_revenue_chart)

# COMMAND ----------

region_revenue_chart = (
    region_df
    .select(
        "region",
        "location",
        "total_revenue"
    )
    .orderBy(F.desc("total_revenue"))
)

display(region_revenue_chart)

# COMMAND ----------

region_chart_data = (
    region_df
    .withColumn(
        "region_location",
        F.concat(
            F.col("region"),
            F.lit(" - "),
            F.col("location")
        )
    )
    .select(
        "region_location",
        "total_revenue"
    )
    .orderBy(F.desc("total_revenue"))
)

display(region_chart_data)

# COMMAND ----------

display(overall_performance)