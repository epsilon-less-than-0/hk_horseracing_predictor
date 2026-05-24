import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def run_theoretical_roi_backtest():
    logging.info("=== PHASE 15.1: THEORETICAL QUINELLA ROI (V8.1) ===")
    logging.info("Strategy: 3-Horse Quinella Box ($30 Risked Per Race)")
    logging.info("Pricing Engine: Normalized Harville Combinatorics (17.5% Takeout)")
    
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

    # 4. Stage 3: Theoretical Pricing Desk (2023-2025)
    logging.info("Running Financial Simulation on 2023-2025 Holdout Data...\n")
    test_df['ranker_raw_score'] = ranker.predict(test_df[features].astype(float))
    test_df['model_rank'] = test_df.groupby(['date', 'race_id'])['ranker_raw_score'].rank(ascending=False, method='min')
    
    TICKET_COST = 10.0
    BETS_PER_RACE = 3       # A 3-horse box = 3 separate tickets
    COST_PER_RACE = TICKET_COST * BETS_PER_RACE
    TAKEOUT = 0.175         # HKJC standard exotic takeout
    
    races_played = 0
    quinellas_hit = 0
    total_action = 0.0
    total_returned = 0.0
    
    races = test_df.groupby(['date', 'race_id'])
    
    for (date, race_id), race_data in races:
        if len(race_data) < 8: continue
        
        # --- THE HARVILLE PRE-PROCESSING ---
        # 1. Convert raw odds to implied probability
        race_data = race_data.copy()
        race_data['implied_prob'] = 1.0 / race_data['win_odds']
        
        # 2. Strip the Track Takeout (Overround) to find True Probability
        pool_total = race_data['implied_prob'].sum()
        race_data['true_prob'] = race_data['implied_prob'] / pool_total
        
        # --- THE EXECUTION ---
        top_3 = race_data[race_data['model_rank'] <= 3.0]
        if len(top_3) < 3: continue
        
        races_played += 1
        total_action += COST_PER_RACE
        
        # Check if we hit the Quinella
        winner = top_3[top_3['plc_num'] == 1.0]
        second = top_3[top_3['plc_num'] == 2.0]
        
        if len(winner) > 0 and len(second) > 0:
            quinellas_hit += 1
            
            # --- THE HARVILLE PAYOUT CALCULATION ---
            p1 = winner.iloc[0]['true_prob']
            p2 = second.iloc[0]['true_prob']
            
            # Prob of Winner finishing 1st and Second finishing 2nd
            prob_1_2 = (p1 * p2) / (1.0 - p1)
            # Prob of Second finishing 1st and Winner finishing 2nd
            prob_2_1 = (p2 * p1) / (1.0 - p2)
            
            # Total combinatorial probability of the Quinella
            quinella_prob = prob_1_2 + prob_2_1
            
            if quinella_prob > 0:
                # Convert probability to Fair Odds
                fair_odds = 1.0 / quinella_prob
                
                # Apply the HKJC 17.5% track rake
                actual_payout_multiplier = fair_odds * (1.0 - TAKEOUT)
                
                # Calculate the cash returned on the $10 winning ticket
                payout = TICKET_COST * actual_payout_multiplier
                total_returned += payout

    # 5. Financial Debrief
    hit_rate = (quinellas_hit / races_played) * 100 if races_played > 0 else 0
    roi = ((total_returned - total_action) / total_action) * 100 if total_action > 0 else 0
    
    logging.info("=== THEORETICAL FINANCIAL DEBRIEF (2023-2025) ===")
    logging.info(f"Total Races Played:  {races_played}")
    logging.info(f"Quinellas Hit:       {quinellas_hit} (Hit Rate: {hit_rate:.2f}%)")
    logging.info(f"Total Action (Vol):  ${total_action:,.2f}")
    logging.info(f"Total Cash Returned: ${total_returned:,.2f}")
    
    if total_returned > total_action:
        logging.info(f"Net Profit:          +${(total_returned - total_action):,.2f}")
        logging.info(f"Yield (ROI):         +{roi:.2f}% (ALPHA CONFIRMED)")
    else:
        logging.info(f"Net Loss:            -${(total_action - total_returned):,.2f}")
        logging.info(f"Yield (ROI):         {roi:.2f}%")

if __name__ == "__main__":
    run_theoretical_roi_backtest()