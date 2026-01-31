import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from delta import *

# Define paths
DATA_DIR = "data"
# Set Hadoop Home for Windows
os.environ['HADOOP_HOME'] = os.path.abspath("hadoop")
os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], 'bin')

STREAM_INPUT_DIR = os.path.join(DATA_DIR, "stream_input")
DELTA_TABLE_PATH = os.path.join(DATA_DIR, "gold_network_telemetry")
CHECKPOINT_DIR = os.path.join(DATA_DIR, "checkpoints", "streaming_pipeline")

def create_spark_session():
    builder = SparkSession.builder \
        .appName("Aegis_Streaming_Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def to_snake_case_stream(df):
    for column in df.columns:
        new_column = column.replace(' ', '_').replace('/', '_').lower()
        df = df.withColumnRenamed(column, new_column)
    return df

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    print(f"[*] Starting Streaming Pipeline monitoring: {STREAM_INPUT_DIR}")
    
    # 1. READ (Streaming Source)
    # Define a schema for the CSVs (needed for streaming)
    # Based on CSE-CIC-IDS2018 or UNSW-NB15 patterns
    # For flexibility, we can use a sample or define some common fields
    # Here we use a generic schema but we'll try to infer if possible (inferSchema=True is slow in streaming)
    
    # Let's read one file to get schema if possible, or define manually
    # For the sake of this demo, we'll assume the schema matches IDS2018 common fields
    
    # 2. DEFINE PIPELINE
    # [FIX]: Spark Structured Streaming requires an explicit schema for file sources.
    schema = StructType([
        StructField("Dst Port", StringType(), True),
        StructField("Protocol", StringType(), True),
        StructField("Timestamp", StringType(), True),
        StructField("Flow Duration", StringType(), True),
        StructField("Tot Fwd Pkts", StringType(), True),
        StructField("Tot Bwd Pkts", StringType(), True),
        StructField("TotLen Fwd Pkts", StringType(), True),
        StructField("TotLen Bwd Pkts", StringType(), True),
        StructField("Fwd Pkt Len Max", StringType(), True),
        StructField("Fwd Pkt Len Min", StringType(), True),
        StructField("Fwd Pkt Len Mean", StringType(), True),
        StructField("Fwd Pkt Len Std", StringType(), True),
        StructField("Bwd Pkt Len Max", StringType(), True),
        StructField("Bwd Pkt Len Min", StringType(), True),
        StructField("Bwd Pkt Len Mean", StringType(), True),
        StructField("Bwd Pkt Len Std", StringType(), True),
        StructField("Flow Byts/s", StringType(), True),
        StructField("Flow Pkts/s", StringType(), True),
        StructField("Flow IAT Mean", StringType(), True),
        StructField("Flow IAT Std", StringType(), True),
        StructField("Flow IAT Max", StringType(), True),
        StructField("Flow IAT Min", StringType(), True),
        StructField("Fwd IAT Tot", StringType(), True),
        StructField("Fwd IAT Mean", StringType(), True),
        StructField("Fwd IAT Std", StringType(), True),
        StructField("Fwd IAT Max", StringType(), True),
        StructField("Fwd IAT Min", StringType(), True),
        StructField("Bwd IAT Tot", StringType(), True),
        StructField("Bwd IAT Mean", StringType(), True),
        StructField("Bwd IAT Std", StringType(), True),
        StructField("Bwd IAT Max", StringType(), True),
        StructField("Bwd IAT Min", StringType(), True),
        StructField("Fwd PSH Flags", StringType(), True),
        StructField("Bwd PSH Flags", StringType(), True),
        StructField("Fwd URG Flags", StringType(), True),
        StructField("Bwd URG Flags", StringType(), True),
        StructField("Fwd Header Len", StringType(), True),
        StructField("Bwd Header Len", StringType(), True),
        StructField("Fwd Pkts/s", StringType(), True),
        StructField("Bwd Pkts/s", StringType(), True),
        StructField("Pkt Len Min", StringType(), True),
        StructField("Pkt Len Max", StringType(), True),
        StructField("Pkt Len Mean", StringType(), True),
        StructField("Pkt Len Std", StringType(), True),
        StructField("Pkt Len Var", StringType(), True),
        StructField("FIN Flag Cnt", StringType(), True),
        StructField("SYN Flag Cnt", StringType(), True),
        StructField("RST Flag Cnt", StringType(), True),
        StructField("PSH Flag Cnt", StringType(), True),
        StructField("ACK Flag Cnt", StringType(), True),
        StructField("URG Flag Cnt", StringType(), True),
        StructField("CWE Flag Count", StringType(), True),
        StructField("ECE Flag Cnt", StringType(), True),
        StructField("Down/Up Ratio", StringType(), True),
        StructField("Pkt Size Avg", StringType(), True),
        StructField("Fwd Seg Size Avg", StringType(), True),
        StructField("Bwd Seg Size Avg", StringType(), True),
        StructField("Fwd Byts/b Avg", StringType(), True),
        StructField("Fwd Pkts/b Avg", StringType(), True),
        StructField("Fwd Blk Rate Avg", StringType(), True),
        StructField("Bwd Byts/b Avg", StringType(), True),
        StructField("Bwd Pkts/b Avg", StringType(), True),
        StructField("Bwd Blk Rate Avg", StringType(), True),
        StructField("Subflow Fwd Pkts", StringType(), True),
        StructField("Subflow Fwd Byts", StringType(), True),
        StructField("Subflow Bwd Pkts", StringType(), True),
        StructField("Subflow Bwd Byts", StringType(), True),
        StructField("Init Fwd Win Byts", StringType(), True),
        StructField("Init Bwd Win Byts", StringType(), True),
        StructField("Fwd Act Data Pkts", StringType(), True),
        StructField("Fwd Seg Size Min", StringType(), True),
        StructField("Active Mean", StringType(), True),
        StructField("Active Std", StringType(), True),
        StructField("Active Max", StringType(), True),
        StructField("Active Min", StringType(), True),
        StructField("Idle Mean", StringType(), True),
        StructField("Idle Std", StringType(), True),
        StructField("Idle Max", StringType(), True),
        StructField("Idle Min", StringType(), True),
        StructField("Label", StringType(), True)
    ])

    df_stream = spark.readStream \
        .option("header", "true") \
        .option("maxFilesPerTrigger", 1) \
        .schema(schema) \
        .csv(STREAM_INPUT_DIR)
        
    # Standardize column names
    df_cleaned = to_snake_case_stream(df_stream)
    
    # Handle Nulls
    df_cleaned = df_cleaned.na.fill("0") # Fill with string "0" since all CSV reads are strings initially
    
    # 3. TRANSFORMATION (Silver/Gold Logic simplified for streaming)
    # Convert types for calculation
    # Only if columns exist
    if "label" in df_cleaned.columns:
        df_silver = df_cleaned.filter(col("label") != "Benign")
    else:
        df_silver = df_cleaned

    # Aggregation in streaming requires a watermark or just outputting as is
    # For a Gold table "Append", we can just store the processed events
    # If we want to maintain the aggregate 'Gold' table from databricks_pipeline.py:
    # df_gold = df_silver.groupBy("dst_port").agg(...)
    # However, for an "alert" style streaming, we typically want the processed records
    
    # Let's stick to the aggregate logic but use Complete mode or Append mode with new records
    # For this project, appending new alerts/events seems more appropriate for a SOC agent
    
    # Transform numeric columns
    df_numeric = df_silver.select(
        col("dst_port").cast(IntegerType()).alias("target_port"),
        col("tot_fwd_pkts").cast(IntegerType()).alias("total_packets"),
        col("totlen_fwd_pkts").cast(DoubleType()).alias("total_bytes"),
        col("label")
    )

    # 4. WRITE (Streaming Sink - Delta Lake)
    print(f"[*] Sinking to: {DELTA_TABLE_PATH}")
    
    # [REAL-WORLD NOTE]: In a high-speed SOC, we use a low trigger interval
    # processingTime='1 second' means Spark checks for new events every second.
    query = df_numeric.writeStream \
        .format("delta") \
        .outputMode("append") \
        .trigger(processingTime='1 second') \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .start(DELTA_TABLE_PATH)
        
    print("[*] Stream started. Waiting for data...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
