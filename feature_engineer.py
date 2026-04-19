import sqlite3
import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def feature_engineer_v2():
    logging.info("=== STARTING PHASE 5: ALPHA GENERATION (V2.0) ===")
    
    # 1. Connect and Load (Using your pristine schema names)
    logging.info("Loading chronological data and human elements from hk_racing.db...")
    conn = sqlite3.connect('hk_racing.db')
    
    query = """
        SELECT 
            date, race_id, horse_id, horse_name, jockey_name, trainer_name,
            race_type as track_info, actual_wt as carried_weight, race_dist as distance, 
            plc, pre_race_elo, draw
        FROM race_results
    """
    df = pd.read_sql(query, conn)
    
    # 2. Strict Chronological Sorting
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by=['date', 'race_id'])
    
    # 3. Clean Numeric Columns
    df['carried_weight'] = pd.to_numeric(df['carried_weight'], errors='coerce')
    df['distance'] = pd.to_numeric(df['distance'], errors='coerce')
    df['plc_num'] = pd.to_numeric(df['plc'], errors='coerce')
    df['draw'] = pd.to_numeric(df['draw'], errors='coerce')
    df['is_turf'] = df['track_info'].astype(str).str.contains('TURF', case=False).astype(int)
    df['is_win'] = (df['plc_num'] == 1).astype(int)
    
    # Handle missing draws (scratched horses or data anomalies) by assigning a neutral middle barrier (7)
    df['draw'] = df['draw'].fillna(7.0)

    logging.info("Calculating Horse Contextual Deltas...")
    
    # 4. Synthesize HORSE Features
    grouped_horse = df.groupby('horse_id')
    
    df['last_race_date'] = grouped_horse['date'].shift(1)
    df['days_since_last_run'] = (df['date'] - df['last_race_date']).dt.days
    df['days_since_last_run'] = df['days_since_last_run'].fillna(30)
    
    df['last_weight'] = grouped_horse['carried_weight'].shift(1)
    df['weight_delta'] = df['carried_weight'] - df['last_weight']
    df['weight_delta'] = df['weight_delta'].fillna(0)
    
    df['last_distance'] = grouped_horse['distance'].shift(1)
    df['distance_delta'] = df['distance'] - df['last_distance']
    df['distance_delta'] = df['distance_delta'].fillna(0)
    
    df['career_wins'] = grouped_horse['is_win'].apply(lambda x: x.shift(1).cumsum()).reset_index(level=0, drop=True)
    df['career_wins'] = df['career_wins'].fillna(0)
    
    logging.info("Calculating Human Momentum (Rolling Win %)...")
    
    # 5. Synthesize JOCKEY Features (Strictly Shifted)
    df['jockey_rides'] = df.groupby('jockey_name').cumcount()
    df['jockey_wins_shifted'] = df.groupby('jockey_name')['is_win'].shift(1).fillna(0)
    df['jockey_cum_wins'] = df.groupby('jockey_name')['jockey_wins_shifted'].cumsum()
    # If rides > 0, calculate true %. Otherwise, assign 8% baseline.
    df['jockey_win_pct'] = np.where(df['jockey_rides'] > 0, df['jockey_cum_wins'] / df['jockey_rides'], 0.08)
    
    # 6. Synthesize TRAINER Features (Strictly Shifted)
    df['trainer_runners'] = df.groupby('trainer_name').cumcount()
    df['trainer_wins_shifted'] = df.groupby('trainer_name')['is_win'].shift(1).fillna(0)
    df['trainer_cum_wins'] = df.groupby('trainer_name')['trainer_wins_shifted'].cumsum()
    df['trainer_win_pct'] = np.where(df['trainer_runners'] > 0, df['trainer_cum_wins'] / df['trainer_runners'], 0.08)

    # 7. Clean up temporary calculation columns
    drop_cols = [
        'last_race_date', 'last_weight', 'last_distance', 'is_win', 'plc_num',
        'jockey_rides', 'jockey_wins_shifted', 'jockey_cum_wins',
        'trainer_runners', 'trainer_wins_shifted', 'trainer_cum_wins'
    ]
    df = df.drop(columns=drop_cols)
    
    # 8. Save back to the SQLite Vault
    logging.info("Saving V2.0 ML Matrix to 'ml_features' table...")
    df.to_sql('ml_features', conn, if_exists='replace', index=False)
    
    # Create Indexes
    cursor = conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ml_date ON ml_features(date);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ml_race ON ml_features(date, race_id);")
    
    # Matrix Summary
    feature_cols = ['pre_race_elo', 'days_since_last_run', 'weight_delta', 'distance_delta', 'career_wins', 'draw', 'jockey_win_pct', 'trainer_win_pct']
    logging.info("\n[V2.0 MATRIX GENERATION COMPLETE]")
    logging.info(f"Generated {len(df)} feature rows.")
    logging.info(f"New Core Features added: 'draw', 'jockey_win_pct', 'trainer_win_pct'")
    
    conn.close()

if __name__ == "__main__":
    feature_engineer_v2()