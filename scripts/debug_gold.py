from connectors import AegisConnector

def check_gold():
    print("--- 🔍 DEBUGGING GOLD TABLE 🔍 ---")
    connector = AegisConnector()
    connector.connect_databricks()
    
    if not connector.db_conn:
        return

    # 1. Check if the IP exists
    target_ip = "18.219.9.1"
    query = f"SELECT * FROM workspace.default.gold_network_telemetry WHERE source_ip = '{target_ip}'"
    
    cursor = connector.db_conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    
    if rows:
        print(f"✅ Found IP {target_ip}: {rows}")
    else:
        print(f"❌ IP {target_ip} NOT FOUND in Gold Table.")
        
        # 2. Check what IS in the table (Limit 5)
        print("Here is what IS in the table:")
        cursor.execute("SELECT * FROM workspace.default.gold_network_telemetry LIMIT 5")
        for row in cursor.fetchall():
            print(f" - {row}")

    connector.close_connections()

if __name__ == "__main__":
    check_gold()
