import sqlite3
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def inject_pace_topology():
    logging.info("=== PHASE 8.1: DATABASE INJECTION (V3 ALPHA MATRIX) ===")
    
    conn = sqlite3.connect('hk_racing.db')
    
    # 1. Load the existing ML Features (V2)
    logging.info("Extracting master ML matrix from vault...")
    df_ml = pd.read_sql("SELECT * FROM ml_features", conn)
    
    # Normalize dates to ensure a mathematically perfect join
    df_ml['date'] = pd.to_datetime(df_ml['date']).dt.normalize()
    
    # 2. Load the Orthogonal Pace Data
    logging.info("Loading topological pace vectors from combined_race_data.csv...")
    try:
        df_csv = pd.read_csv('combined_race_data.csv')
    except FileNotFoundError:
        logging.error("combined_race_data.csv not found! Aborting.")
        return

    df_csv['date'] = pd.to_datetime(df_csv['date'], dayfirst=True, errors='coerce').dt.normalize()
    
    # Isolate ONLY the primary keys and the new Phase 8 columns
    pace_columns = [
        'date', 'race_id', 'horse_id', 
        'shifted_rolling_ESI', 'shifted_rolling_CSI', 
        'race_ESI_pressure', 'pace_advantage'
    ]
    
    # Filter the CSV to only what we need, dropping rows with bad dates to prevent merge pollution
    df_pace = df_csv.dropna(subset=['date'])[pace_columns].copy()
    
    # Ensure race_id and horse_id are strictly strings for reliable merging
    df_ml['race_id'] = df_ml['race_id'].astype(str)
    df_ml['horse_id'] = df_ml['horse_id'].astype(str)
    df_pace['race_id'] = df_pace['race_id'].astype(str)
    df_pace['horse_id'] = df_pace['horse_id'].astype(str)

    # 3. The Surgical Merge
    logging.info(f"Merging {len(df_pace)} pace vectors into {len(df_ml)} matrix rows...")
    
    # We use a LEFT JOIN to preserve every row in our ML matrix. 
    # If pace data is missing (e.g. earliest races), it becomes NaN.
    df_v3 = pd.merge(df_ml, df_pace, on=['date', 'race_id', 'horse_id'], how='left')
    
    # 4. Save Back to the Vault
    logging.info("Overwriting 'ml_features' table with V3 architecture...")
    df_v3.to_sql('ml_features', conn, if_exists='replace', index=False)
    
    # Re-establish Indexes for query speed
    cursor = conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ml_date ON ml_features(date);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ml_race ON ml_features(date, race_id);")
    
    logging.info("\n=== INJECTION COMPLETE ===")
    logging.info(f"Final V3 Matrix Shape: {df_v3.shape}")
    
    # Audit the new columns
    new_cols = ['shifted_rolling_ESI', 'shifted_rolling_CSI', 'race_ESI_pressure', 'pace_advantage']
    for col in new_cols:
        missing = df_v3[col].isna().sum()
        logging.info(f"Column '{col}' verified. (Null values: {missing})")
        
    conn.close()

if __name__ == "__main__":
    inject_pace_topology()