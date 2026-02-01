from connectors import AegisConnector
import sys

def verify_live_connections():
    print("--- ☁️ STARTING LIVE CLOUD VERIFICATION ☁️ ---")
    
    # 1. Initialize Connector (This triggers .connect_snowflake() and .connect_databricks())
    try:
        print("\n[1/3] Establishing Connections...")
        connector = AegisConnector()
    except Exception as e:
        print(f"❌ CRITICAL FAILURE: Could not initialization connector. {e}")
        sys.exit(1)

    # 2. Test Snowflake (Real Data)
    print("\n[2/3] Testing Snowflake (SIEM)...")
    try:
        user_id = 1
        user_data = connector.fetch_user_context(user_id)
        if user_data:
            print(f"   ✅ SUCCESS: Found User: {user_data}")
        else:
            print(f"   ⚠️ WARNING: Connected, but found no user for ID {user_id}. (Is tables populated?)")
    except Exception as e:
        print(f"   ❌ FAILURE: Snowflake Query Failed. {e}")

    # 3. Test Databricks (Real Data)
    print("\n[3/3] Testing Databricks (Data Lake)...")
    try:
        ip = "192.168.1.105" # This IP matches our synthetic injection in the notebook
        traffic_data = connector.get_traffic_volume(ip)
        
        if traffic_data:
            vol = traffic_data[0][0]
            print(f"   ✅ SUCCESS: Retrieved Traffic Volume: {vol} bytes")
        elif traffic_data == []:
            print("   ✅ SUCCESS: Connected, but query returned empty (No logs for this IP).")
        else:
            print("   ❌ FAILURE: Null result from Databricks.")
            
    except Exception as e:
        print(f"   ❌ FAILURE: Databricks Query Failed. {e}")

    print("\n--- 🏁 VERIFICATION COMPLETE ---")

if __name__ == "__main__":
    verify_live_connections()
