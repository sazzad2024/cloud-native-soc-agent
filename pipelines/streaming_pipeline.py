import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from delta import *

# Define paths (Relative to project root)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Set Hadoop Home for Windows
os.environ['HADOOP_HOME'] = os.path.join(BASE_DIR, "hadoop")
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
        
    df_cleaned = to_snake_case_stream(df_stream)
    df_cleaned = df_cleaned.na.fill("0") 
    
    # 3. TRANSFORMATION (Filter for Alerts)
    df_silver = df_cleaned.filter(col("label") != "Benign")

    df_numeric = df_silver.select(
        col("dst_port").cast(IntegerType()).alias("target_port"),
        col("tot_fwd_pkts").cast(IntegerType()).alias("total_packets"),
        col("totlen_fwd_pkts").cast(DoubleType()).alias("total_bytes"),
        col("label")
    )

    # 4. WRITE (Streaming Sink - Delta Lake)
    print(f"[*] Sinking to: {DELTA_TABLE_PATH}")
    
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
