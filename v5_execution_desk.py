import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def run_calibrated_kelly_desk():
    logging.info("=== PHASE 11: TWO-STAGE CALIBRATED KELLY DESK (V5.0) ===")
    
    # 1. Load Both Models
    try:
        ranker = xgb.XGBRanker()
        ranker.load_model('v3_hkjc_ranker.json')
        calibrator = xgb.XGBClassifier()
        calibrator.load_model('v5_hkjc_calibrator.json')
        logging.info("Loaded V3 Targeter and V5 Calibrator.")
    except Exception as e:
        logging.error("Could not load models. Check JSON files.")
        return

    # 2. Extract Data
    conn = sqlite3.connect('hk_racing.db')
    df_features = pd.read_sql("SELECT * FROM ml_features", conn)
    df_odds = pd.read_sql("SELECT date, race_id, horse_id, public_odds as win_odds FROM race_results", conn)
    conn.close()

    df_features['date'] = pd.to_datetime(df_features['date']).dt.normalize()
    df_odds['date'] = pd.to_datetime(df_odds['date']).dt.normalize()
    df = pd.merge(df_features, df_odds, on=['date', 'race_id', 'horse_id'], how='inner')
    df = df.drop_duplicates(subset=['date', 'race_id', 'horse_id'])

    df['plc_num'] = pd.to_numeric(df['plc'], errors='coerce')
    df['win_odds'] = pd.to_numeric(df['win_odds'], errors='coerce')

    features = [
        'pre_race_elo', 'days_since_last_run', 'weight_delta', 
        'distance_delta', 'career_wins', 'is_turf', 
        'draw', 'jockey_win_pct', 'trainer_win_pct',
        'shifted_rolling_ESI', 'shifted_rolling_CSI', 
        'race_ESI_pressure', 'pace_advantage'
    ]

    # Filter for Unseen Holdout Data (2025+)
    test_df = df[(df['date'].dt.year >= 2025)].dropna(subset=['plc_num', 'win_odds'] + features).copy()
    test_df = test_df.sort_values(by=['date', 'race_id'])
    
    if len(test_df) == 0: return

    # 3. Stage 1: The Targeter
    logging.info("Stage 1: Targeter scanning for Alpha...")
    X_test = test_df[features].astype(float)
    test_df['ranker_raw_score'] = ranker.predict(X_test)
    test_df['model_rank'] = test_df.groupby(['date', 'race_id'])['ranker_raw_score'].rank(ascending=False, method='min')
    test_df['public_rank'] = test_df.groupby(['date', 'race_id'])['win_odds'].rank(ascending=True, method='min')
    
    # 4. Stage 2: The Calibrator
    logging.info("Stage 2: Meta-Model calibrating true probabilities...")
    test_df['public_implied_prob'] = 1.0 / test_df['win_odds']
    
    calibrator_features = ['ranker_raw_score', 'public_implied_prob', 'pace_advantage']
    X_calib = test_df[calibrator_features]
    
    # Predict exact probability [:, 1] for the positive class (Win)
    test_df['calibrated_prob'] = calibrator.predict_proba(X_calib)[:, 1]

    # 5. The Fractional Kelly Trading Desk
    STARTING_BANKROLL = 10000.0
    bankroll = STARTING_BANKROLL
    KELLY_FRACTION = 0.25 # Quarter Kelly
    MAX_BET_CAP = 0.05    # Hard cap at 5%
    
    bets_placed = 0
    winners_hit = 0
    total_action = 0.0
    
    logging.info("\n=== INITIATING DYNAMIC KELLY EXECUTION ===")
    logging.info("Ruleset:")
    logging.info("- Filter: Bet only #1 AI Picks in Divergence Zone (Public Rank >= 3)")
    logging.info("- Probability: Meta-Model True Calibrated Probability")
    logging.info("- Risk: Quarter Kelly (Max 5% Bankroll Cap)")
    logging.info("-" * 55)
    
    races = test_df.groupby(['date', 'race_id'])
    
    for (date, race_id), race_data in races:
        top_pick = race_data[race_data['model_rank'] == 1.0]
        if len(top_pick) == 0: continue
        
        horse = top_pick.iloc[0]
        
        # THE FILTER: Starve the bad bets
        if horse['public_rank'] < 3.0:
            continue
            
        # THE MATH: Calibrated Kelly Edge Calculation
        p = horse['calibrated_prob']
        b = horse['win_odds'] - 1.0
        q = 1.0 - p
        
        if b <= 0: continue
        
        f_star = p - (q / b)
        
        # Only bet if Expected Value is positive
        if f_star > 0:
            bet_pct = f_star * KELLY_FRACTION
            bet_pct = min(bet_pct, MAX_BET_CAP)
            
            wager = bankroll * bet_pct
            
            bankroll -= wager
            total_action += wager
            bets_placed += 1
            
            if horse['plc_num'] == 1.0:
                payout = wager * horse['win_odds']
                bankroll += payout
                winners_hit += 1

    # 6. Final Trading Desk Debrief
    roi = ((bankroll - STARTING_BANKROLL) / total_action) * 100 if total_action > 0 else 0
    win_rate = (winners_hit / bets_placed) * 100 if bets_placed > 0 else 0
    
    logging.info("\n=== FINAL TRADING DESK DEBRIEF ===")
    logging.info(f"Total Action (Vol): ${total_action:,.2f}")
    logging.info(f"Bets Executed:      {bets_placed}")
    logging.info(f"Winners Hit:        {winners_hit} (Win Rate: {win_rate:.1f}%)")
    logging.info(f"Starting Bankroll:  ${STARTING_BANKROLL:,.2f}")
    logging.info(f"Ending Bankroll:    ${bankroll:,.2f}")
    
    if bankroll > STARTING_BANKROLL:
        logging.info(f"Net Profit:         +${(bankroll - STARTING_BANKROLL):,.2f}")
        logging.info(f"Yield (ROI):        +{roi:.2f}% (BEAT THE TAKEOUT!)")
    else:
        logging.info(f"Net Loss:           -${(STARTING_BANKROLL - bankroll):,.2f}")
        logging.info(f"Yield (ROI):        {roi:.2f}%")

if __name__ == "__main__":
    run_calibrated_kelly_desk()