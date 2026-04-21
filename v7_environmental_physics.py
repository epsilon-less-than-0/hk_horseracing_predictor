import sqlite3
import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def engineer_environmental_physics():
    logging.info("=== PHASE 13: ENVIRONMENTAL PHYSICS UPGRADE (V7.0) ===")
    
    conn = sqlite3.connect('hk_racing.db')
    
    # Extract existing features (which contains 'track_info')
    logging.info("Extracting Pace Topology and Track Info...")
    df = pd.read_sql("SELECT * FROM ml_features", conn)
    
    # Extract 'going' from race_results
    df_env = pd.read_sql("SELECT date, race_id, going FROM race_results", conn)
    df_env = df_env.drop_duplicates(subset=['date', 'race_id'])
    
    # Merge 'going' into our main dataframe
    df = pd.merge(df, df_env, on=['date', 'race_id'], how='left')
    
    # 1. Course Mapping (Track Bias) via Regex Extraction
    # Search the 'track_info' string for "A", "B", "C", "C+3", etc.
    logging.info("Extracting Course Geometry from track_info strings...")
    df['course_clean'] = df['track_info'].astype(str).str.extract(r'([A-C](?:\+3|\+2)?)')[0]
    
    course_map = {
        'A': 1.0,
        'A+3': 1.05,
        'B': 1.10,
        'B+2': 1.15,
        'C': 1.20,
        'C+3': 1.30
    }
    df['course_multiplier'] = df['course_clean'].map(course_map).fillna(1.0) # Default to 1.0 if not found/All Weather
    
    # 2. Going Mapping (Surface Friction)
    going_map = {
        'GOOD TO FIRM': 1.0,
        'FAST': 1.0,
        'GOOD': 0.9,
        'GOOD TO YIELDING': 0.8,
        'YIELDING': 0.7,
        'SOFT': 0.6,
        'WET FAST': 0.8
    }
    
    df['going_clean'] = df['going'].astype(str).str.upper().str.strip()
    df['going_multiplier'] = df['going_clean'].map(going_map).fillna(0.9) # Default to Good
    
    # 3. Orthogonal Physics Engineering
    logging.info("Calculating ESI_Track_Bias and CSI_Surface_Friction...")
    
    # Interact Early Speed with Track Bias
    df['ESI_Track_Bias'] = df['shifted_rolling_ESI'] * df['course_multiplier']
    
    # Interact Closing Speed with Surface Friction
    df['CSI_Surface_Friction'] = df['shifted_rolling_CSI'] * df['going_multiplier']
    
    # 4. Save to the Vault
    logging.info("Injecting V7 Features into the Database...")
    
    # Drop intermediate columns to keep DB clean
    cols_to_drop = ['course_clean', 'course_multiplier', 'going_clean', 'going_multiplier', 'going']
    df = df.drop(columns=cols_to_drop, errors='ignore')
    
    # Save as a new table so we don't overwrite the original V5 features
    df.to_sql('ml_features_v7', conn, if_exists='replace', index=False)
    
    conn.close()
    
    logging.info("SUCCESS: Environmental Pace Geometry added.")
    logging.info("New Features Available: ['ESI_Track_Bias', 'CSI_Surface_Friction']")

if __name__ == "__main__":
    engineer_environmental_physics()