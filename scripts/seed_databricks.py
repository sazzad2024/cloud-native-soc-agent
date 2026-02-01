from connectors import AegisConnector
import sys

def seed_databricks():
    print("--- 🌱 SEEDING DATABRICKS TABLE 🌱 ---")
    
    try:
        connector = AegisConnector()
        connector.connect_databricks() # <--- FIXED: Must connect explicitly
        
        # Ensure we have a valid connection
        if not connector.db_conn:
            print("❌ Error: Could not connect to Databricks.")
            return

        cursor = connector.db_conn.cursor()
        
        # 1. Create Table
        print("1. Creating Table `gold_network_telemetry`...")
        create_sql = """
        CREATE TABLE IF NOT EXISTS gold_network_telemetry (
            source_ip STRING,
            total_packets BIGINT,
            total_bytes BIGINT,
            risk_score INT
        );
        """
        cursor.execute(create_sql)
        
        # 2. Clear Old Data
        print("2. Clearing old data...")
        cursor.execute("TRUNCATE TABLE gold_network_telemetry")
        
        # 3. Insert Sample Data
        # Simulating the result of the Spark ETL Pipeline
        print("3. Inserting sample production data...")
        insert_sql = """
        INSERT INTO gold_network_telemetry VALUES
        ('192.168.1.105', 500, 500000, 85),  -- ALICE (High Traffic)
        ('10.0.0.5', 10, 2000, 10);          -- BOB (Low Traffic)
        """
        cursor.execute(insert_sql)
        
        # 4. Verify
        print("4. Verifying...")
        cursor.execute("SELECT count(*) FROM gold_network_telemetry")
        count = cursor.fetchall()[0][0]
        print(f"✅ Success! Table now has {count} rows.")
        
        cursor.close()
        connector.close_connections()
        
    except Exception as e:
        print(f"❌ Seed Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    seed_databricks()
