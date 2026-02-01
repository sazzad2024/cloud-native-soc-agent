import os
import time
import pandas as pd

SOURCE_CSV = os.path.join("data", "02-14-2018.csv")
TARGET_DIR = os.path.join("data", "stream_input")

def simulate():
    if not os.path.exists(SOURCE_CSV):
        print(f"Error: {SOURCE_CSV} not found.")
        return

    print(f"[*] Reading source data: {SOURCE_CSV}")
    # Grab a mix of Benign and Malicious records
    df_benign = pd.read_csv(SOURCE_CSV, nrows=10)
    df_malicious = pd.read_csv(SOURCE_CSV, skiprows=range(1, 95), nrows=10) # Grabbing records near index 96
    
    df = pd.concat([df_malicious, df_benign]).reset_index(drop=True)
    
    print(f"[*] Starting balanced simulation. Dropping 1 alert per second (Mixed Benign/Malicious)...")
    
    for i, row in df.iterrows():
        # Create a single-row CSV
        filename = f"alert_stream_{i}.csv"
        filepath = os.path.join(TARGET_DIR, filename)
        
        # Save only this row
        row_df = pd.DataFrame([row])
        row_df.to_csv(filepath, index=False)
        
        print(f"   -> Sent {row['Label']} Alert (Original Index: {i})")
        time.sleep(1) # Simulate 1 second delay between alerts

if __name__ == "__main__":
    simulate()
