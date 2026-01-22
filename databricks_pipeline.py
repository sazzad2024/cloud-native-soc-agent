import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum
from delta import *

# Define paths
DATA_DIR = "data"
# Set Hadoop Home for Windows
os.environ['HADOOP_HOME'] = os.path.abspath("hadoop")
os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], 'bin')

INPUT_CSV = os.path.join(DATA_DIR, "02-14-2018.csv") # Or whatever file we download
DELTA_TABLE_PATH = os.path.join(DATA_DIR, "gold_network_telemetry")

def create_spark_session():
    builder = SparkSession.builder \
        .appName("Databricks_Simulation_Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        # .master("local[*]") # Use local master

    return configure_spark_with_delta_pip(builder).getOrCreate()

def to_snake_case(df):
    for column in df.columns:
        new_column = column.replace(' ', '_').replace('/', '_').lower()
        df = df.withColumnRenamed(column, new_column)
    return df

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    # 1. READ (Bronze)
    print("Reading data from:", INPUT_CSV)
    # Check if file exists first
    if not os.path.exists(INPUT_CSV):
        print(f"Error: Input file {INPUT_CSV} not found.")
        return

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(INPUT_CSV)
    
    # 2. TRANSFORM (Cleaning)
    print("Standardizing column names...")
    df_cleaned = to_snake_case(df)
    
    # Handle Nulls (Simple imputation for this demo)
    df_cleaned = df_cleaned.na.fill(0)
    
    print("\n--- Bronze Layer Sample ---")
    df_cleaned.printSchema()
    
    # 3. SILVER LAYER (Filter/Enrich)
    # Logic: Keep traffic where Label != 'Benign'
    print("\nFiltering for non-benign traffic (Silver Layer)...")
    
    # CSE-CIC-IDS2018 usually has a 'label' column
    if 'label' in df_cleaned.columns:
        df_silver = df_cleaned.filter(col("label") != "Benign")
    else:
        print("Warning: 'label' column not found, skipping filter.")
        df_silver = df_cleaned

    count_silver = df_silver.count()
    print(f"Silver Layer Record Count: {count_silver}")

    # 4. GOLD LAYER (Aggregation)
    # Goal: Aggregate by dst_port (Since IPs were stripped in this dataset version)
    # Metrics: Count pkts, Sum bytes, Count attacks
    
    print("\nAggregating metrics (Gold Layer)...")
    
    # Use Dst Port as the grouping key since Src IP is missing
    group_col = "dst_port" if "dst_port" in df_silver.columns else None
    
    if group_col:
        df_gold = df_silver.groupBy(group_col).agg(
            _sum("tot_fwd_pkts").alias("total_packets"),
            _sum("totlen_fwd_pkts").alias("total_bytes"),
            col("dst_port").alias("target_port") # Redundant but keeps schema similar
        )
        
        print("\n--- Gold Layer Sample ---")
        df_gold.show(5)
        
        # 5. WRITE TO DELTA TABLE
        print(f"Saving Gold Table to {DELTA_TABLE_PATH}...")
        
        # Clean up existing table for this run if needed
        if os.path.exists(DELTA_TABLE_PATH):
             shutil.rmtree(DELTA_TABLE_PATH)
             
        df_gold.write.format("delta").mode("overwrite").save(DELTA_TABLE_PATH)
        print("Saved successfully.")
        
        # Verify read back
        print("\nVerifying Delta Table Read:")
        df_verify = spark.read.format("delta").load(DELTA_TABLE_PATH)
        df_verify.show(3)
        
    else:
        print(f"Error: Grouping column '{group_col}' not found.")

if __name__ == "__main__":
    main()
