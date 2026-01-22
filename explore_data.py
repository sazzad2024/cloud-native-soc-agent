from connectors import AegisConnector
import sys

def count_ips():
    print("--- 🔍 DATA EXPLORATION 🔍 ---")
    try:
        connector = AegisConnector()
        connector.connect_databricks()
        
        if not connector.db_conn:
            print("❌ Connections failed.")
            return

        cursor = connector.db_conn.cursor()
        
        print("Querying unique Source IPs in Bronze Logs...")
        # Note: Column name typically has spaces inferred from CSV header, using backticks
        query = "SELECT count(DISTINCT `Src IP`) FROM workspace.default.bronze_traffic_logs"
        
        cursor.execute(query)
        result = cursor.fetchone()
        
        print(f"✅ Total Unique Source IPs: {result[0]}")
        
        # Optional: List top 5
        print("\nTop 5 Source IPs by Volume:")
        query_top = """
        SELECT `Src IP`, count(*) as cnt 
        FROM workspace.default.bronze_traffic_logs 
        GROUP BY `Src IP` 
        ORDER BY cnt DESC 
        LIMIT 5
        """
        cursor.execute(query_top)
        rows = cursor.fetchall()
        for row in rows:
            print(f" - {row[0]}: {row[1]} packets")

        cursor.close()
        connector.close_connections()

    except Exception as e:
        print(f"❌ Query Failed: {e}")

if __name__ == "__main__":
    count_ips()
