import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def run_quinella_proof_of_concept():
    logging.info("=== PHASE 15: EXOTICS COMBINATORIAL DESK (V8.0) ===")
    logging.info("Strategy: 3-Horse Quinella Box (Targeting 1st and 2nd place)")
    
    # 1. Extract V7 Environmental Data
    conn = sqlite3.connect('hk_racing.db')
    df_features = pd.read_sql("SELECT * FROM ml_features_v7", conn)
    df_odds = pd.read_sql("SELECT date, race_id, horse_id, public_odds as win_odds FROM race_results", conn)
    conn.close()

    df_features['date'] = pd.to_datetime(df_features['date']).dt.normalize()
    df_odds['date'] = pd.to_datetime(df_odds['date']).dt.normalize()
    df = pd.merge(df_features, df_odds, on=['date', 'race_id', 'horse_id'], how='inner')
    df = df.drop_duplicates(subset=['date', 'race_id', 'horse_id'])

    df['plc_num'] = pd.to_numeric(df['plc'], errors='coerce')
    df['win_odds'] = pd.to_numeric(df['win_odds'], errors='coerce')
    df['relevance'] = (15 - df['plc_num']).clip(lower=0)

    features = [
        'pre_race_elo', 'days_since_last_run', 'weight_delta', 
        'distance_delta', 'career_wins', 'is_turf', 
        'draw', 'jockey_win_pct', 'trainer_win_pct',
        'shifted_rolling_ESI', 'shifted_rolling_CSI', 
        'race_ESI_pressure', 'pace_advantage',
        'ESI_Track_Bias', 'CSI_Surface_Friction'
    ]

    df = df.dropna(subset=['plc_num', 'win_odds'] + features).copy()
    df = df.sort_values(by=['date', 'race_id', 'relevance'], ascending=[True, True, False])

    # 2. The Macro Splits
    train_df = df[df['date'].dt.year <= 2021].copy()
    test_df  = df[df['date'].dt.year >= 2023].copy()

    # 3. Stage 1: Train the Targeter (Ranker)
    logging.info("\nTraining XGBRanker with Environmental Physics (2018-2021)...")
    X_train, y_train = train_df[features], train_df['relevance']
    group_train = train_df.groupby(['date', 'race_id'], sort=False).size().values
    
    ranker = xgb.XGBRanker(
        tree_method='hist', objective='rank:pairwise', eval_metric='ndcg',
        learning_rate=0.01, max_depth=5, subsample=0.8, colsample_bytree=0.5, n_estimators=1000
    )
    ranker.fit(X_train, y_train, group=group_train, verbose=False)

    # 4. Stage 3: Quinella Execution Desk (2023-2025)
    logging.info("Running Quinella Execution on 2023-2025 Holdout Data...")
    test_df['ranker_raw_score'] = ranker.predict(test_df[features].astype(float))
    test_df['model_rank'] = test_df.groupby(['date', 'race_id'])['ranker_raw_score'].rank(ascending=False, method='min')
    
    races_played = 0
    quinellas_hit = 0
    
    races = test_df.groupby(['date', 'race_id'])
    
    for (date, race_id), race_data in races:
        # We need a full race to evaluate
        if len(race_data) < 8: continue
        
        # Take the Top 3 AI Picks (The Quinella Box)
        top_3_picks = race_data[race_data['model_rank'] <= 3.0]
        if len(top_3_picks) < 3: continue
        
        races_played += 1
        
        # Did our Top 3 Box contain BOTH the 1st and 2nd place horse?
        actual_winner = len(top_3_picks[top_3_picks['plc_num'] == 1.0]) > 0
        actual_second = len(top_3_picks[top_3_picks['plc_num'] == 2.0]) > 0
        
        if actual_winner and actual_second:
            quinellas_hit += 1

    # 5. Combinatorial Debrief
    hit_rate = (quinellas_hit / races_played) * 100 if races_played > 0 else 0
    
    # Mathematical Baseline: In a 12 horse race, picking 3 horses to fill the top 2 spots by random chance is ~4.5%
    logging.info("\n=== COMBINATORIAL EXOTICS DEBRIEF ===")
    logging.info(f"Total Races Played: {races_played}")
    logging.info(f"Quinellas Hit:      {quinellas_hit}")
    logging.info(f"Hit Rate:           {hit_rate:.2f}%")
    logging.info(f"Random Chance:      ~4.50%")
    
    if hit_rate > 9.0:
        logging.info("\nVERDICT: MASSIVE EXOTIC EDGE DETECTED.")
        logging.info("Your AI is predicting the exact race flow at more than double random chance.")
    else:
        logging.info("\nVERDICT: WEAK EXOTIC EDGE.")

if __name__ == "__main__":
    run_quinella_proof_of_concept()