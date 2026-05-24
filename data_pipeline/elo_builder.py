import pandas as pd
import numpy as np
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Elo Constants
BASE_ELO = 1500
K_FACTOR = 32

def expected_score(rating_a, rating_b):
    """Calculates the expected probability of Horse A beating Horse B."""
    return 1 / (1 + 10 ** ((rating_b - rating_a) / 400))

def calculate_multi_elo(df):
    logging.info("Loading and preparing data...")
    
    # 1. Chronological Sorting (Critical for sequential Elo)
    df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
    df = df.sort_values(by=['date', 'race_id'])
    
    # 2. Strict Numeric Enforcement for Placings
    # Converts "WV", "DNF", or missing values to NaN so they don't break the math
    df['plc_num'] = pd.to_numeric(df['plc'], errors='coerce')
    
    # Global Elo tracking and DataFrame Index mapping
    horse_elos = {}
    pre_race_elos = {}
    post_race_elos = {}
    
    grouped = df.groupby(['date', 'race_id'])
    logging.info(f"Processing {len(grouped)} unique races for Elo calculations...")
    
    for (date, race_id), race_data in grouped:
        # Dictionary keyed by exact DataFrame index, NOT horse_id
        current_race_horses = {}
        
        for idx, row in race_data.iterrows():
            h_id = row['horse_id']
            
            # Skip invalid rows (e.g., simulcast dividend rows)
            if pd.isna(h_id):
                continue
                
            # Initialize new horses at 1500
            if h_id not in horse_elos:
                horse_elos[h_id] = BASE_ELO
                
            current_race_horses[idx] = {
                'h_id': h_id,
                'rating': horse_elos[h_id],
                'plc': row['plc_num'],
                'delta': 0
            }
        
        # Only calculate pairwise math for horses that actually finished the race
        valid_idxs = [i for i, data in current_race_horses.items() if not pd.isna(data['plc'])]
        N = len(valid_idxs)
        
        # 3. Calculate Pairwise Matchups N(N-1)/2
        if N > 1:
            for i in range(N):
                idx_a = valid_idxs[i]
                id_a = current_race_horses[idx_a]['h_id']
                rating_a = current_race_horses[idx_a]['rating']
                plc_a = current_race_horses[idx_a]['plc']
                
                for j in range(i + 1, N):
                    idx_b = valid_idxs[j]
                    id_b = current_race_horses[idx_b]['h_id']
                    rating_b = current_race_horses[idx_b]['rating']
                    plc_b = current_race_horses[idx_b]['plc']
                    
                    # Expected Score
                    exp_a = expected_score(rating_a, rating_b)
                    exp_b = 1 - exp_a
                    
                    # Actual Score (1 = win, 0 = loss, 0.5 = dead heat)
                    if plc_a < plc_b:
                        score_a, score_b = 1, 0
                    elif plc_a > plc_b:
                        score_a, score_b = 0, 1
                    else: 
                        score_a, score_b = 0.5, 0.5
                        
                    # Calculate Deltas
                    current_race_horses[idx_a]['delta'] += K_FACTOR * (score_a - exp_a)
                    current_race_horses[idx_b]['delta'] += K_FACTOR * (score_b - exp_b)
        
        # 4. Apply Updates and Record History
        for idx, data in current_race_horses.items():
            h_id = data['h_id']
            old_rating = data['rating']
            
            # Only apply the delta if they raced (N>1) and weren't scratched
            if N > 1 and not pd.isna(data['plc']):
                avg_delta = data['delta'] / (N - 1)
                new_rating = old_rating + avg_delta
            else:
                # Scratched horses keep their old rating
                new_rating = old_rating
                
            horse_elos[h_id] = new_rating
            
            # Map back to specific DataFrame index
            pre_race_elos[idx] = old_rating
            post_race_elos[idx] = new_rating
            
    # 5. Safe Pandas Assignment
    # pd.Series aligns automatically with the DataFrame's original index structure
    df['pre_race_elo'] = pd.Series(pre_race_elos).round(2)
    df['post_race_elo'] = pd.Series(post_race_elos).round(2)
    df['elo_shift'] = df['post_race_elo'] - df['pre_race_elo']
    
    # Drop the temporary calculation column and drop empty simulcast rows entirely
    df = df.drop(columns=['plc_num'])
    df = df.dropna(subset=['horse_id'])
    
    logging.info("Elo calculations complete!")
    return df

if __name__ == "__main__":
    input_file = "combined_race_data.csv"
    output_file = "elo_augmented_race_data.csv"
    
    try:
        # low_memory=False suppresses the mixed-type warning you saw
        df = pd.read_csv(input_file, low_memory=False) 
        elo_df = calculate_multi_elo(df)
        
        # Save the result
        elo_df.to_csv(output_file, index=False)
        logging.info(f"Saved Elo-augmented dataset to {output_file}")
        
    except FileNotFoundError:
        logging.error(f"Could not find {input_file}.")