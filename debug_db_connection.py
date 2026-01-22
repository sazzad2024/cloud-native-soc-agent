import os
from dotenv import load_dotenv
from databricks import sql

def debug_connection():
    print("--- 🕵️ DEBUG CONNECTION 🕵️ ---")
    load_dotenv()
    
    host = os.getenv("DATABRICKS_HOST")
    path = os.getenv("DATABRICKS_HTTP_PATH")
    token = os.getenv("DATABRICKS_TOKEN")
    
    # Masking for safety
    print(f"HOST: {host}")
    print(f"PATH: {path}")
    print(f"TOKEN: {token[:5]}...{token[-5:]}" if token else "TOKEN: None")
    
    if not host or "mock" in host:
        print("❌ CRITICAL: Host is missing or still set to mock!")
        return

    print("\nAttempting raw connection...")
    try:
        conn = sql.connect(
            server_hostname=host,
            http_path=path,
            access_token=token,
            _retry_policy=None
        )
        print("✅ Connection Established!")
        
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        print("✅ Query 'SELECT 1' Successful!")
        conn.close()
        
    except Exception as e:
        print(f"❌ CONNECTION FAILED: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_connection()
