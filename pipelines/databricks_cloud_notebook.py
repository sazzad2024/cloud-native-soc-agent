# Databricks notebook source
# MAGIC %md
# MAGIC # SOC Agent Data Pipeline
# MAGIC This notebook implements the Medallion Architecture for network telemetry data.

# COMMAND ----------

from pyspark.sql.functions import col, when, sum as _sum

# Define paths (Assumes data is uploaded to FileStore)
INPUT_CSV_PATH = "dbfs:/FileStore/tables/02-14-2018.csv"
GOLD_TABLE_NAME = "gold_network_telemetry"

# COMMAND ----------

def to_snake_case(df):
    for column in df.columns:
        new_column = column.replace(' ', '_').replace('/', '_').lower()
        df = df.withColumnRenamed(column, new_column)
    return df

# COMMAND ----------

# 1. READ (Bronze Layer)
# Using 'spark' implicit session
print(f"Reading data from: {INPUT_CSV_PATH}")

try:
    df = spark.table("workspace.default.02_14_2018") 
    display(df.limit(5))
except Exception as e:
    print(f"Error reading file: {e}")
    dbutils.notebook.exit("File not found")

# COMMAND ----------

# 2. TRANSFORM (Cleaning)
print("Standardizing column names...")
df_cleaned = to_snake_case(df)
df_cleaned = df_cleaned.na.fill(0)

display(df_cleaned.limit(5))

# COMMAND ----------

# 3. SILVER LAYER (Exploration / Filter)
# In production, we keep ALL traffic logs (Benign & Malicious) for forensics.
# We do NOT filter by 'Label' because real routers don't label data.
print("Processing all network traffic...")
df_silver = df_cleaned

# Realism: Drop the academic 'label' column if it exists (Simulating raw logs)
if 'label' in df_silver.columns:
    df_silver = df_silver.drop('label')

print(f"Silver Layer Record Count: {df_silver.count()}")
display(df_silver.limit(5))

# COMMAND ----------

# 4. GOLD LAYER (Aggregation)
print("Aggregating metrics...")

# SYNTHETIC DATA INJECTION (For Demo Realism)
# Since the public dataset anonymized IPs, we will simulate them to match our User Database.
# We randomly assign IPs: 50% chance of Alice's IP (192.168.1.105), 50% others.
from pyspark.sql.functions import lit, rand, when

df_silver_enriched = df_silver.withColumn(
    "src_ip", 
    when(rand() > 0.5, lit("192.168.1.105")) # Alice (User ID 1)
    .otherwise(lit("10.0.0.5"))              # Bob (User ID 2)
)

# Group by the User's IP to find "Top Talkers"
group_col = "src_ip"

if group_col:
    df_gold = df_silver_enriched.groupBy(group_col).agg(
        _sum("tot_fwd_pkts").alias("total_packets"),
        _sum("totlen_fwd_pkts").alias("total_bytes"),
        col("src_ip").alias("source_ip")
    )
    
    display(df_gold)
    
    # 5. WRITE TO DELTA TABLE
    print(f"Saving as Delta Table: {GOLD_TABLE_NAME}")
    df_gold.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE_NAME)
    print("Success.")
    
else:
    print(f"Error: Grouping column '{group_col}' not found.")
