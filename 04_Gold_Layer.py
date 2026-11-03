# Databricks notebook source
from pyspark.sql import functions as F

# Read enriched Silver data
sales_df = spark.table(
    "workspace.silver.sales_project_enriched_sales"
)

# Consider only completed orders
completed_sales_df = sales_df.filter(
    F.col("order_status") == "Completed"
)

# Create monthly KPI aggregation
monthly_kpis_df = (
    completed_sales_df
    .withColumn("year", F.year("order_date"))
    .withColumn("month", F.month("order_date"))
    .withColumn("year_month", F.date_format("order_date", "yyyy-MM"))
    .groupBy("year", "month", "year_month")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("net_sales"), 2).alias("total_revenue"),
        F.round(F.sum("product_cost"), 2).alias("total_cost"),
        F.round(F.sum("profit"), 2).alias("total_profit")
    )
    .withColumn(
        "profit_margin_pct",
        F.when(
            F.col("total_revenue") != 0,
            F.round(
                F.col("total_profit") /
                F.col("total_revenue") * 100, 2
            )
        ).otherwise(0)
    )
    .orderBy("year", "month")
)

# Save Gold table
monthly_kpis_df.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.gold.sales_project_monthly_kpis"
)

print("Monthly KPI table created successfully!")
display(monthly_kpis_df)

# COMMAND ----------

from pyspark.sql import functions as F

sales_df = spark.table(
    "workspace.silver.sales_project_enriched_sales"
)

completed_sales_df = sales_df.filter(
    F.col("order_status") == "Completed"
)

product_kpis_df = (
    completed_sales_df
    .groupBy("product_id", "product_name", "category")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
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
                F.col("total_profit") /
                F.col("total_revenue") * 100, 2
            )
        ).otherwise(0)
    )
    .orderBy(F.desc("total_revenue"))
)

product_kpis_df.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.gold.sales_project_product_kpis"
)

print("Product KPI table created successfully!")
display(product_kpis_df)

# COMMAND ----------

from pyspark.sql import functions as F

sales_df = spark.table(
    "workspace.silver.sales_project_enriched_sales"
)

completed_sales_df = sales_df.filter(
    F.col("order_status") == "Completed"
)

category_kpis_df = (
    completed_sales_df
    .groupBy("category")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
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
                F.col("total_profit") /
                F.col("total_revenue") * 100, 2
            )
        ).otherwise(0)
    )
    .orderBy(F.desc("total_revenue"))
)

category_kpis_df.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.gold.sales_project_category_kpis"
)

print("Category KPI table created successfully!")
display(category_kpis_df)

# COMMAND ----------

from pyspark.sql import functions as F

sales_df = spark.table(
    "workspace.silver.sales_project_enriched_sales"
)

completed_sales_df = sales_df.filter(
    F.col("order_status") == "Completed"
)

customer_kpis_df = (
    completed_sales_df
    .groupBy("customer_id", "segment", "location")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity_purchased"),
        F.round(F.sum("net_sales"), 2).alias("total_spending"),
        F.round(F.sum("product_cost"), 2).alias("total_cost"),
        F.round(F.sum("profit"), 2).alias("total_profit"),
        F.round(F.avg("net_sales"), 2).alias("average_order_value")
    )
    .orderBy(F.desc("total_spending"))
)

customer_kpis_df.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.gold.sales_project_customer_kpis"
)

print("Customer KPI table created successfully!")
display(customer_kpis_df)

# COMMAND ----------

from pyspark.sql import functions as F

sales_df = spark.table(
    "workspace.silver.sales_project_enriched_sales"
)

completed_sales_df = sales_df.filter(
    F.col("order_status") == "Completed"
)

region_kpis_df = (
    completed_sales_df
    .groupBy("region", "location")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
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
                F.col("total_profit") /
                F.col("total_revenue") * 100, 2
            )
        ).otherwise(0)
    )
    .orderBy(F.desc("total_revenue"))
)

region_kpis_df.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.gold.sales_project_region_kpis"
)

print("Region KPI table created successfully!")
display(region_kpis_df)

# COMMAND ----------

from pyspark.sql import functions as F

sales_df = spark.table(
    "workspace.silver.sales_project_enriched_sales"
)

completed_sales_df = sales_df.filter(
    F.col("order_status") == "Completed"
)

source_kpis_df = (
    completed_sales_df
    .groupBy("order_source")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
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
                F.col("total_profit") /
                F.col("total_revenue") * 100, 2
            )
        ).otherwise(0)
    )
    .orderBy(F.desc("total_revenue"))
)

source_kpis_df.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.gold.sales_project_source_kpis"
)

print("Sales Source KPI table created successfully!")
display(source_kpis_df)

# COMMAND ----------

gold_tables = [
    "sales_project_monthly_kpis",
    "sales_project_product_kpis",
    "sales_project_category_kpis",
    "sales_project_customer_kpis",
    "sales_project_region_kpis",
    "sales_project_source_kpis"
]

for table_name in gold_tables:
    full_table_name = f"workspace.gold.{table_name}"
    df = spark.table(full_table_name)

    print(
        f"{table_name} | Records: {df.count()}"
    )