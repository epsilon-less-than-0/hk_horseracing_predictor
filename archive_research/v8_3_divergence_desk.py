import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def run_divergence_roi_backtest():
    logging.info("=== PHASE 17: THE DIVERGENCE DESK (V8.3) ===")
    logging.info("Strategy: 3-Horse Box ($30 Flat Risk)")
    logging.info("Filter 1: The Dark Horse Rule (Require at least one Public Rank >= 5)")
    
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

    train_df = df[df['date'].dt.year <= 2021].copy()
    test_df  = df[df['date'].dt.year >= 2023].copy()

    logging.info("\nTraining XGBRanker with Environmental Physics (2018-2021)...")
    X_train, y_train = train_df[features], train_df['relevance']
    group_train = train_df.groupby(['date', 'race_id'], sort=False).size().values
    
    ranker = xgb.XGBRanker(
        tree_method='hist', objective='rank:pairwise', eval_metric='ndcg',
        learning_rate=0.01, max_depth=5, subsample=0.8, colsample_bytree=0.5, n_estimators=1000
    )
    ranker.fit(X_train, y_train, group=group_train, verbose=False)

    logging.info("Running Financial Simulation on 2023-2025 Holdout Data...\n")
    test_df['ranker_raw_score'] = ranker.predict(test_df[features].astype(float))
    test_df['model_rank'] = test_df.groupby(['date', 'race_id'])['ranker_raw_score'].rank(ascending=False, method='min')
    test_df['public_rank'] = test_df.groupby(['date', 'race_id'])['win_odds'].rank(ascending=True, method='min')
    
    TICKET_COST = 10.0
    BETS_PER_RACE = 3       
    COST_PER_RACE = TICKET_COST * BETS_PER_RACE
    TAKEOUT = 0.175         
    
    races_evaluated = 0
    races_played = 0
    quinellas_hit = 0
    total_action = 0.0
    total_returned = 0.0
    
    races = test_df.groupby(['date', 'race_id'])
    
    for (date, race_id), race_data in races:
        if len(race_data) < 8: continue
        races_evaluated += 1
        
        race_data = race_data.copy()
        race_data['implied_prob'] = 1.0 / race_data['win_odds']
        pool_total = race_data['implied_prob'].sum()
        race_data['true_prob'] = race_data['implied_prob'] / pool_total
        
        top_3 = race_data[race_data['model_rank'] <= 3.0]
        if len(top_3) < 3: continue
        
        # --- FILTER 1: THE DIVERGENCE RULE ---
        # Check if at least one of our top 3 picks is a Dark Horse (Public Rank >= 5)
        has_dark_horse = (top_3['public_rank'] >= 5.0).any()
        
        if not has_dark_horse:
            continue # Pass on the race. Too much chalk.
            
        races_played += 1
        total_action += COST_PER_RACE
        
        winner = top_3[top_3['plc_num'] == 1.0]
        second = top_3[top_3['plc_num'] == 2.0]
        
        if len(winner) > 0 and len(second) > 0:
            quinellas_hit += 1
            
            p1 = winner.iloc[0]['true_prob']
            p2 = second.iloc[0]['true_prob']
            
            prob_1_2 = (p1 * p2) / (1.0 - p1)
            prob_2_1 = (p2 * p1) / (1.0 - p2)
            quinella_prob = prob_1_2 + prob_2_1
            
            if quinella_prob > 0:
                fair_odds = 1.0 / quinella_prob
                actual_payout_multiplier = fair_odds * (1.0 - TAKEOUT)
                payout = TICKET_COST * actual_payout_multiplier
                total_returned += payout

    hit_rate = (quinellas_hit / races_played) * 100 if races_played > 0 else 0
    roi = ((total_returned - total_action) / total_action) * 100 if total_action > 0 else 0
    
    logging.info("=== DIVERGENCE DESK DEBRIEF (2023-2025) ===")
    logging.info(f"Total Races Evaluated: {races_evaluated}")
    logging.info(f"Races Played (Passed): {races_played} (Passed {races_evaluated - races_played})")
    logging.info(f"Quinellas Hit:         {quinellas_hit} (Hit Rate: {hit_rate:.2f}%)")
    logging.info(f"Total Action (Vol):    ${total_action:,.2f}")
    logging.info(f"Total Cash Returned:   ${total_returned:,.2f}")
    
    if total_returned > total_action:
        logging.info(f"Net Profit:            +${(total_returned - total_action):,.2f}")
        logging.info(f"Yield (ROI):           +{roi:.2f}% (ALPHA CONFIRMED)")
    else:
        logging.info(f"Net Loss:              -${(total_action - total_returned):,.2f}")
        logging.info(f"Yield (ROI):           {roi:.2f}%")

if __name__ == "__main__":
    run_divergence_roi_backtest()