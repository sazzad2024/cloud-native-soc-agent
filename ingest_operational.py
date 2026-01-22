import os
import pandas as pd
import psycopg2
from psycopg2 import sql
import random

# Database connection parameters - ADJUST AS NEEDED
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "postgres" # Common default, might need to be changed
DB_HOST = "localhost"
DB_PORT = "5432"

DATA_DIR = "data"
CSV_FILE = os.path.join(DATA_DIR, "UNSW_NB15_training-set.csv")

def connect_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def create_tables(conn):
    cur = conn.cursor()
    
    # Create active_alerts table
    # Schema: id, dur, proto, service, state, spkts, dpkts, sbytes, dbytes, attack_cat, label
    cur.execute("""
        CREATE TABLE IF NOT EXISTS active_alerts (
            id SERIAL PRIMARY KEY,
            dur FLOAT,
            proto VARCHAR(50),
            service VARCHAR(50),
            state VARCHAR(50),
            spkts INTEGER,
            dpkts INTEGER,
            sbytes INTEGER,
            dbytes INTEGER,
            attack_cat VARCHAR(100),
            label INTEGER
        );
    """)
    
    # Create user_directory table
    # Schema: user_id, email, department, risk_score
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_directory (
            user_id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            department VARCHAR(100),
            risk_score FLOAT
        );
    """)
    
    conn.commit()
    cur.close()
    print("Tables created successfully.")

def load_data(conn):
    cur = conn.cursor()
    
    if not os.path.exists(CSV_FILE):
        print(f"File not found: {CSV_FILE}")
        return

    print(f"Loading data from {CSV_FILE}...")
    df = pd.read_csv(CSV_FILE)
    
    # Map CSV columns to table columns if necessary. 
    # Based on UNSW-NB15, usually headers are provided.
    # Selecting relevant columns
    cols_to_keep = ['id', 'dur', 'proto', 'service', 'state', 'spkts', 'dpkts', 'sbytes', 'dbytes', 'attack_cat', 'label']
    
    # Check if 'id' exists in CSV, otherwise let DB handle SERIAL or use index
    if 'id' in df.columns:
        df_selected = df[cols_to_keep]
    else:
        # If 'id' is not in CSV, we map the rest and let postgres handle id if it was SERIAL, 
        # BUT 'id' is in the schema requirement. 
        # UNSW-NB15 usually has an 'id' column.
        df_selected = df[cols_to_keep]

    # Insert loop - for bulk speed we could use copy_from but insert is safer for this scale for now
    # Using execute_values for better performance than single inserts
    from psycopg2.extras import execute_values
    
    data_tuples = [tuple(x) for x in df_selected.to_numpy()]
    
    insert_query = """
        INSERT INTO active_alerts (id, dur, proto, service, state, spkts, dpkts, sbytes, dbytes, attack_cat, label)
        VALUES %s
        ON CONFLICT (id) DO NOTHING;
    """
    
    execute_values(cur, insert_query, data_tuples)
    
    # Mock User Directory Data
    mock_users = [
        ("alice@company.com", "HR", 12.5),
        ("bob@company.com", "Engineering", 45.0),
        ("charlie@company.com", "Finance", 8.0),
        ("david@company.com", "IT", 25.0),
        ("eve@company.com", "Executive", 5.0)
    ]
    
    user_query = """
        INSERT INTO user_directory (email, department, risk_score)
        VALUES %s
        ON CONFLICT (email) DO NOTHING;
    """
    
    execute_values(cur, user_query, mock_users)
    
    conn.commit()
    cur.close()
    print("Data loaded successfully.")

def main():
    conn = connect_db()
    if conn:
        create_tables(conn)
        load_data(conn)
        conn.close()

if __name__ == "__main__":
    main()
