# Databricks notebook source
# Portfolio Project: E-Commerce Sales Pipeline
# Layer: Bronze — Raw Ingestion
# Author: [Miqdad]

# COMMAND ----------

# Bronze Layer — Raw Data Ingestion
# Ingest synthetic e-commerce data and store as Delta table (raw, unmodified).

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Generate synthetic e-commerce data

# COMMAND ----------

random.seed(42)

CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Beauty", "Toys"]
STATUSES   = ["completed", "cancelled", "returned", "pending"]
N_RECORDS  = 50_000

def make_orders(n):
    rows = []
    base = datetime(2023, 1, 1)
    for i in range(n):
        order_dt = base + timedelta(days=random.randint(0, 364),
                                    hours=random.randint(0, 23))
        rows.append((
            f"ORD-{i+1:06d}",
            f"CUST-{random.randint(1, 5000):05d}",
            f"PROD-{random.randint(1, 500):04d}",
            random.choice(CATEGORIES),
            round(random.uniform(5.0, 1200.0), 2),
            random.randint(1, 5),
            random.choice(STATUSES),
            order_dt.strftime("%Y-%m-%d %H:%M:%S"),
            f"DE-{random.randint(10000, 99999)}",   # German postal code
        ))
    return rows

schema = StructType([
    StructField("order_id",      StringType(),  True),
    StructField("customer_id",   StringType(),  True),
    StructField("product_id",    StringType(),  True),
    StructField("category",      StringType(),  True),
    StructField("unit_price",    DoubleType(),  True),
    StructField("quantity",      IntegerType(), True),
    StructField("status",        StringType(),  True),
    StructField("order_datetime",StringType(),  True),  # raw string
    StructField("postal_code",   StringType(),  True),
])

raw_df = spark.createDataFrame(make_orders(N_RECORDS), schema=schema)

# COMMAND ----------

# Write to Bronze Delta table
# Store raw data as-is — no transformations at this layer.

# COMMAND ----------

(    raw_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("bronze_orders")
)

print(f"Bronze layer written: {raw_df.count():,} records")
spark.sql("SELECT * FROM bronze_orders LIMIT 5").show(truncate=False)
