# Databricks notebook source
# Portfolio Project: E-Commerce Sales Pipeline
# Layer: Gold — Business Aggregations

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer — Business Metrics
# MAGIC Build analysis-ready aggregated tables for dashboards and reporting.

# COMMAND ----------

from pyspark.sql import functions as F

silver_df = spark.table("silver_orders").filter(F.col("status") == "completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 1: Monthly Revenue by Category

# COMMAND ----------

monthly_revenue = (
    silver_df
    .groupBy("order_year", "order_month", "category")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.count("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
    )
    .withColumn("year_month", F.concat_ws("-",
        F.col("order_year"),
        F.lpad(F.col("order_month"), 2, "0")
    ))
    .orderBy("order_year", "order_month", F.desc("total_revenue"))
)

(
    monthly_revenue.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("monthly_revenue_by_category")
)

print("Gold: monthly_revenue_by_category written")
monthly_revenue.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 2: Top Products

# COMMAND ----------

top_products = (
    silver_df
    .groupBy("product_id", "category")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.sum("quantity").alias("total_units_sold"),
        F.count("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_buyers"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .orderBy(F.desc("total_revenue"))
)

(
    top_products.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("top_products")
)

print("Gold: top_products written")
top_products.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 3: Customer Segmentation (RFM simplified)

# COMMAND ----------

from pyspark.sql.window import Window

max_date = silver_df.agg(F.max("order_date")).collect()[0][0]

rfm = (
    silver_df
    .groupBy("customer_id")
    .agg(
        F.datediff(F.lit(max_date), F.max("order_date")).alias("recency_days"),
        F.count("order_id").alias("frequency"),
        F.round(F.sum("total_amount"), 2).alias("monetary"),
    )
)

# Simple segment labels based on frequency + monetary
customer_segments = (
    rfm
    .withColumn("segment", F.when(
        (F.col("frequency") >= 5) & (F.col("monetary") >= 1000), "VIP"
    ).when(
        (F.col("frequency") >= 3) & (F.col("monetary") >= 500), "Loyal"
    ).when(
        F.col("recency_days") <= 30, "New"
    ).otherwise("At Risk"))
)

(
    customer_segments.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("customer_segments")
)

print("Gold: customer_segments written")
customer_segments.groupBy("segment").count().orderBy(F.desc("count")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=== Gold Layer Summary ===")
for tbl, label in [
    ("monthly_revenue_by_category",   "Monthly revenue by category"),
    ("top_products",      "Top products"),
    ("customer_segments", "Customer segments"),
]:
    count = spark.table(tbl).count()
    print(f"  {label:35s}: {count:,} records")