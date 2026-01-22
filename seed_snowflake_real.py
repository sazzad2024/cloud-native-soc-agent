from connectors import AegisConnector
import sys

def seed_real_alert():
    print("--- 🚨 SEEDING SNOWFLAKE WITH REAL ALERT 🚨 ---")
    
    # 1. Initialize Connector
    connector = AegisConnector()
    connector.connect_snowflake()
    
    if not connector.sf_conn:
        print("❌ Could not connect to Snowflake. Check credentials.")
        return

    try:
        cursor = connector.sf_conn.cursor()
        
        # 2. Insert the REAL Alert (matching Databricks logs)
        # We use ID 1003 (New Target from Screenshot)
        # IP: 18.218.229.235 (Confirmed visible in your Gold Table screenshot)
        insert_query = """
        INSERT INTO FACT_ALERTS (alert_id, src_ip, attack_cat, severity, user_id, timestamp)
        VALUES (1003, '18.218.229.235', 'Exploits', 'High', 1, CURRENT_TIMESTAMP())
        """
        
        print(f"Executing: {insert_query.strip()}")
        cursor.execute(insert_query)
        connector.sf_conn.commit()
        
        print("✅ SUCCESS: Alert 1002 (IP: 18.219.9.1) injected into Snowflake.")
        print("   The Agent will now naturally pick this up from the SIEM.")

    except Exception as e:
        print(f"❌ Seed Failed: {e}")
    finally:
        connector.close_connections()

if __name__ == "__main__":
    seed_real_alert()
