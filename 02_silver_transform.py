# Databricks notebook source
# Portfolio Project: E-Commerce Sales Pipeline
# Layer: Silver — Cleaning & Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer — Clean, Type, Deduplicate
# MAGIC Read from Bronze, apply quality rules, and produce a clean typed table.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze

# COMMAND ----------

bronze_df = spark.table("bronze_orders")
print(f"Bronze records: {bronze_df.count():,}")
bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks (Bronze → Silver rules)

# COMMAND ----------

total      = bronze_df.count()
nulls      = bronze_df.filter(F.col("order_id").isNull()).count()
duplicates = total - bronze_df.dropDuplicates(["order_id"]).count()
neg_price  = bronze_df.filter(F.col("unit_price") <= 0).count()

print("=== Data Quality Report ===")
print(f"Total records   : {total:,}")
print(f"Null order_ids  : {nulls:,}")
print(f"Duplicate orders: {duplicates:,}")
print(f"Invalid prices  : {neg_price:,}")
print("===========================")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform: cast types, derive columns, filter bad records

# COMMAND ----------

silver_df = (
    bronze_df
    # 1. Cast types
    .withColumn("order_datetime", F.to_timestamp("order_datetime", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("unit_price",     F.col("unit_price").cast("decimal(10,2)"))
    .withColumn("quantity",       F.col("quantity").cast("integer"))

    # 2. Derive columns
    .withColumn("total_amount",   F.round(F.col("unit_price") * F.col("quantity"), 2))
    .withColumn("order_date",     F.to_date("order_datetime"))
    .withColumn("order_year",     F.year("order_datetime"))
    .withColumn("order_month",    F.month("order_datetime"))
    .withColumn("order_quarter",  F.quarter("order_datetime"))
    .withColumn("is_weekend",     F.dayofweek("order_datetime").isin([1, 7]))

    # 3. Filter invalid records
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("unit_price") > 0)
    .filter(F.col("quantity") > 0)
    .filter(F.col("status").isin(["completed", "cancelled", "returned", "pending"]))

    # 4. Deduplicate
    .dropDuplicates(["order_id"])

    # 5. Standardize strings
    .withColumn("category", F.initcap(F.trim("category")))
    .withColumn("status",   F.lower(F.trim("status")))
)

print(f"Silver records after cleaning: {silver_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver Delta table

# COMMAND ----------

(
    silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("order_year", "order_month")
    .saveAsTable("silver_orders")
)


print("Silver layer ready.")
spark.sql("""
    SELECT order_id, customer_id, category, unit_price, quantity,
           total_amount, order_date, status
    FROM silver_orders LIMIT 5
""").show(truncate=False)