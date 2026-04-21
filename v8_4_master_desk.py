import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def run_master_syndicate_desk():
    logging.info("=== PHASE 18: THE MASTER EXOTIC DESK (V8.4) ===")
    logging.info("Strategy: Fractional Quinella Box (Shattering the Box)")
    logging.info("Filter 1: Divergence (Race must contain Public Rank >= 5)")
    logging.info("Filter 2: Value (Individual Combinations must pay > $45)")
    logging.info("Sizing: 1% Bankroll per Valid Combination")
    
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

    logging.info("Running Master Syndicate Execution on 2023-2025 Holdout Data...\n")
    test_df['ranker_raw_score'] = ranker.predict(test_df[features].astype(float))
    test_df['model_rank'] = test_df.groupby(['date', 'race_id'])['ranker_raw_score'].rank(ascending=False, method='min')
    test_df['public_rank'] = test_df.groupby(['date', 'race_id'])['win_odds'].rank(ascending=True, method='min')
    
    STARTING_BANKROLL = 10000.0
    bankroll = STARTING_BANKROLL
    TAKEOUT = 0.175
    MIN_PAYOUT_THRESHOLD = 45.0
    BASE_BET_FRACTION = 0.01 # 1% of Bankroll per valid combination
    
    races_evaluated = 0
    races_played = 0
    combinations_evaluated = 0
    combinations_bet = 0
    winning_tickets = 0
    total_action = 0.0
    
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
        has_dark_horse = (top_3['public_rank'] >= 5.0).any()
        if not has_dark_horse:
            continue
            
        h1, h2, h3 = top_3.iloc[0], top_3.iloc[1], top_3.iloc[2]
        combos = [(h1, h2), (h1, h3), (h2, h3)]
        
        race_action = 0.0
        race_payout = 0.0
        bet_placed = False
        
        for horse_A, horse_B in combos:
            combinations_evaluated += 1
            
            pA = horse_A['true_prob']
            pB = horse_B['true_prob']
            
            quinella_prob = ((pA * pB) / (1.0 - pA)) + ((pB * pA) / (1.0 - pB))
            if quinella_prob <= 0: continue
                
            fair_odds = 1.0 / quinella_prob
            actual_multiplier = fair_odds * (1.0 - TAKEOUT)
            expected_payout = 10.0 * actual_multiplier 
            
            # --- FILTER 2: INDIVIDUAL COMBINATION VALUE RULE ---
            if expected_payout < MIN_PAYOUT_THRESHOLD:
                continue 
                
            # SIZING: Safe 1% Bankroll Compounding
            wager = bankroll * BASE_BET_FRACTION
            
            race_action += wager
            combinations_bet += 1
            bet_placed = True
            
            # Resolution
            if (horse_A['plc_num'] in [1.0, 2.0]) and (horse_B['plc_num'] in [1.0, 2.0]) and (horse_A['plc_num'] != horse_B['plc_num']):
                winning_tickets += 1
                race_payout += (wager * actual_multiplier)
                
        if bet_placed:
            races_played += 1
            bankroll -= race_action
            bankroll += race_payout
            total_action += race_action

    roi = ((bankroll - STARTING_BANKROLL) / total_action) * 100 if total_action > 0 else 0
    hit_rate = (winning_tickets / combinations_bet) * 100 if combinations_bet > 0 else 0
    
    logging.info("=== MASTER SYNDICATE DEBRIEF (2023-2025) ===")
    logging.info(f"Races Evaluated:      {races_evaluated}")
    logging.info(f"Races Engaged:        {races_played} (Passed {races_evaluated - races_played})")
    logging.info(f"Combinations Bet:     {combinations_bet} (Filtered out {combinations_evaluated - combinations_bet} bad combos)")
    logging.info(f"Winning Tickets:      {winning_tickets} (Combo Hit Rate: {hit_rate:.2f}%)")
    logging.info(f"Total Action (Vol):   ${total_action:,.2f}")
    logging.info(f"Starting Bankroll:    ${STARTING_BANKROLL:,.2f}")
    logging.info(f"Ending Bankroll:      ${bankroll:,.2f}")
    
    if bankroll > STARTING_BANKROLL:
        logging.info(f"Net Profit:           +${(bankroll - STARTING_BANKROLL):,.2f}")
        logging.info(f"Yield (ROI):          +{roi:.2f}% (ALPHA CONFIRMED)")
    else:
        logging.info(f"Net Loss:             -${(STARTING_BANKROLL - bankroll):,.2f}")
        logging.info(f"Yield (ROI):          {roi:.2f}%")

if __name__ == "__main__":
    run_master_syndicate_desk()