import os
import snowflake.connector
from databricks import sql
from guardrails import SQLGuardrail, SecurityException
from dotenv import load_dotenv

# Load secrets from .env
load_dotenv()

class AegisConnector:
    """
    Manages connections to Cloud Data Platforms (Snowflake & Databricks).
    Enforces security guardrails on all queries.
    """
    
    def __init__(self):
        self.sf_conn = None
        self.db_conn = None
        self.guard = SQLGuardrail() # Instantiate guardrail
        
    def connect_snowflake(self):
        """Connects to Snowflake (SIEM / Data Warehouse)."""
        try:
            self.sf_conn = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
                database=os.getenv("SNOWFLAKE_DATABASE", "SOC_DATA"),
                schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
                login_timeout=20 # Give enough time for real connection
            )
            print("[Connector] Connected to Snowflake (Real).")
        except Exception as e:
            print(f"[Connector] Snowflake Connection Failed: {e}")

    def connect_databricks(self):
        """Connects to Databricks SQL (Traffic Analysis / Lakehouse)."""
        try:
            self.db_conn = sql.connect(
                server_hostname=os.getenv("DATABRICKS_HOST", "mock-workspace.cloud.databricks.com"),
                http_path=os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/mock"),
                access_token=os.getenv("DATABRICKS_TOKEN", "mock_token"),
                _retry_policy=None # FAIL FAST FOR DEMO
            )
            print("[Connector] Connected to Databricks.")
        except Exception as e:
            print(f"[Connector] Databricks Connection Failed (Expected if no creds): {e}")

    def fetch_user_context(self, user_id: int):
        """
        Fetches user details from Snowflake (DIM_USERS table).
        """
        if not self.sf_conn:
            self.connect_snowflake()
            
        query = f"SELECT * FROM DIM_USERS WHERE user_id = {user_id}"
        
        # Enforce Guardrail on the constructed query
        self.guard.validate_query(query)
        
        if self.sf_conn:
            try:
                cur = self.sf_conn.cursor()
                cur.execute(query)
                return cur.fetchone()
            except Exception as e:
                print(f"[Connector] Snowflake Query Error: {e}")
                return None
        
        print("[Connector] Error: No active Snowflake connection.")
        return None

    def get_alert_by_id(self, alert_id: int):
        """
        Fetches a specific alert by ID from Snowflake.
        """
        if not self.sf_conn:
            self.connect_snowflake()
            
        query = f"SELECT * FROM FACT_ALERTS WHERE alert_id = {alert_id}"
        self.guard.validate_query(query)
        
        if self.sf_conn:
            try:
                cur = self.sf_conn.cursor()
                cur.execute(query)
                return cur.fetchall()
            except Exception as e:
                print(f"[Connector] Snowflake Query Error: {e}")
                return None
        return None

    def get_recent_alerts(self, limit: int = 10):
        """
        Fetches recent alerts from Snowflake (FACT_ALERTS table).
        """
        if not self.sf_conn:
            self.connect_snowflake()
            
        # ORDER BY timestamp DESC to get the latest alert
        query = f"SELECT * FROM FACT_ALERTS ORDER BY timestamp DESC LIMIT {limit}"
        self.guard.validate_query(query)
        
        if self.sf_conn:
            try:
                cur = self.sf_conn.cursor()
                cur.execute(query)
                return cur.fetchall()
            except Exception as e:
                print(f"[Connector] Snowflake Query Error: {e}")
                return None
                
        return None

    def get_traffic_volume(self, ip_address: str):
        """
        Fetches usage stats from Databricks (gold_traffic table).
        """
        if not self.db_conn:
            self.connect_databricks()
            
        query = f"SELECT total_bytes FROM gold_network_telemetry WHERE source_ip = '{ip_address}'"
        self.guard.validate_query(query)
        
        if self.db_conn:
            try:
                cur = self.db_conn.cursor()
                cur.execute(query)
                return cur.fetchall()
            except Exception as e:
                print(f"[Connector] Databricks Query Error: {e}")
                return None
        
        print("[Connector] Error: No active Databricks connection.")
        return None

    def close_connections(self):
        if self.sf_conn:
            self.sf_conn.close()
        if self.db_conn:
            self.db_conn.close()
