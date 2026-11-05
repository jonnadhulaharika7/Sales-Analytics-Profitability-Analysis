# Sales Analytics & Profitability Analysis using PySpark

## Project Overview

This project focuses on building an end-to-end Sales Analytics and Profitability Analysis solution using **Databricks and PySpark**. It follows the **Medallion Architecture (Bronze, Silver, and Gold)** to process raw sales data, perform data cleaning and validation, calculate financial metrics, and generate business insights.

The project analyzes sales performance across products, customers, categories, regions, and sales channels.

## Technologies Used

- Databricks Free Edition
- Apache Spark 4.2.0
- PySpark
- Python
- Delta Tables
- Medallion Architecture

## Project Architecture

The project follows three data processing layers:

### 1. Bronze Layer
- Ingested raw sales and product data.
- Created Bronze tables for storing raw datasets.
- Preserved source data for downstream processing.

### 2. Silver Layer
- Performed data cleaning and standardization.
- Checked duplicate and invalid records.
- Validated data quality.
- Joined sales data with product master data.
- Calculated net sales, product cost, profit, and profit margin.

### 3. Gold Layer
Created business-level aggregated tables for reporting and analysis:

- Monthly KPIs
- Product KPIs
- Category KPIs
- Customer KPIs
- Regional KPIs
- Sales Channel KPIs

## Project Notebooks

| Notebook | Description |
|---|---|
| 01_Data_Generation | Generated sales and product datasets |
| 02_Bronze_Layer | Loaded raw data into Bronze tables |
| 03_Silver_Layer | Cleaned, validated, and enriched data |
| 04_Gold_Layer | Created business KPI aggregation tables |
| 05_Sales_Analysis | Performed business analysis and visualizations |

## Business Analysis

The project includes the following analyses:

1. Top 10 revenue-generating products
2. Top 10 most profitable products
3. Monthly revenue and profit trends
4. Top 10 highest-spending customers
5. Category-wise profitability
6. Regional sales performance
7. Sales channel performance
8. Overall business performance

## Key Business Results

| KPI | Result |
|---|---:|
| Total Sales Records | 1,000 |
| Product Records | 50 |
| Completed Orders | 600 |
| Total Quantity Sold | 3,352 |
| Total Revenue | 3,184,644.26 |
| Total Cost | 2,314,614.22 |
| Total Profit | 870,030.04 |
| Profit Margin | 27.32% |

## Key Business Insights

- Mobile App generated the highest revenue among sales channels.
- Desserts generated the highest category-level profit.
- Snacks recorded the highest category-level revenue.
- Chips 13 was the highest-profit product among the top 10 products.
- April 2025 recorded the highest monthly revenue.
- North India recorded the highest revenue among region-country combinations.

## Visualizations

Created visualizations in Databricks to analyze:

- Monthly Revenue Trend
- Top 10 Most Profitable Products
- Category-wise Revenue
- Category-wise Profit
- Sales Channel Revenue
- Regional Revenue Performance
- Overall Business KPIs

## Project Outcomes

- Implemented an end-to-end PySpark data processing workflow.
- Applied Bronze, Silver, and Gold architecture.
- Performed data quality checks and transformations.
- Developed reusable business KPI tables.
- Analyzed sales and profitability across multiple business dimensions.
- Generated actionable business insights using Databricks visualizations.

## Author

**Harika Jonnadhula**
