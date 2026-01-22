from connectors import AegisConnector

def check_snowflake():
    print("--- ❄️ SNOWFLAKE DATA AUDIT ❄️ ---")
    connector = AegisConnector()
    connector.connect_snowflake()
    
    if not connector.sf_conn:
        print("❌ Connect Failed.")
        return

    cursor = connector.sf_conn.cursor()
    
    # 1. Check Alerts
    print("\n[FACT_ALERTS] (The Incident Reports):")
    cursor.execute("SELECT alert_id, src_ip, attack_cat, severity FROM FACT_ALERTS ORDER BY timestamp DESC LIMIT 5")
    for row in cursor.fetchall():
        print(f" - Alert {row[0]}: {row[2]} attack from IP {row[1]} ({row[3]})")

    # 2. Check Users
    print("\n[DIM_USERS] (The Employee Directory):")
    cursor.execute("SELECT user_id, email, department FROM DIM_USERS LIMIT 5")
    for row in cursor.fetchall():
        print(f" - User {row[0]}: {row[1]} ({row[2]})")

    connector.close_connections()

if __name__ == "__main__":
    check_snowflake()
