import os
from pyspark.sql import SparkSession
from delta import *

# Define paths
DATA_DIR = "data"
os.environ['HADOOP_HOME'] = os.path.abspath("hadoop")
os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], 'bin')

DELTA_TABLE_PATH = os.path.join(DATA_DIR, "gold_network_telemetry")

def check_local_gold():
    builder = SparkSession.builder \
        .appName("Check_Local_Gold") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .master("local[*]")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"\n--- Checking Local Delta Table: {DELTA_TABLE_PATH} ---")
    if os.path.exists(DELTA_TABLE_PATH):
        df = spark.read.format("delta").load(DELTA_TABLE_PATH)
        print(f"Total Records: {df.count()}")
        print("\nLast 5 Records:")
        df.show(5)
    else:
        print("Local Gold table not found.")

    spark.stop()

if __name__ == "__main__":
    check_local_gold()
