import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def run_sector_kelly_desk():
    logging.info("=== PHASE 9: SECTOR-RESTRICTED KELLY DESK (V4.0) ===")
    
    # 1. Load V3 Pace-Adjusted Model
    try:
        ranker = xgb.XGBRanker()
        ranker.load_model('v3_hkjc_ranker.json')
    except Exception as e:
        logging.error("Could not load V3 model. Please ensure 'v3_hkjc_ranker.json' exists.")
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

    test_df = df[(df['date'].dt.year >= 2025)].dropna(subset=['plc_num', 'win_odds'] + features).copy()
    test_df = test_df.sort_values(by=['date', 'race_id'])
    
    # 3. Generate Predictions & Probabilities
    logging.info("Calculating Ranker Scores and Softmax Probabilities...")
    X_test = test_df[features].astype(float)
    test_df['raw_score'] = ranker.predict(X_test)
    
    # Calculate Intra-Race Softmax Probability
    def softmax(x):
        e_x = np.exp(x - np.max(x))
        return e_x / e_x.sum()
        
    test_df['model_prob'] = test_df.groupby(['date', 'race_id'])['raw_score'].transform(softmax)
    test_df['model_rank'] = test_df.groupby(['date', 'race_id'])['raw_score'].rank(ascending=False, method='min')
    test_df['public_rank'] = test_df.groupby(['date', 'race_id'])['win_odds'].rank(ascending=True, method='min')
    
    # 4. The Fractional Kelly Trading Desk
    STARTING_BANKROLL = 10000.0
    bankroll = STARTING_BANKROLL
    KELLY_FRACTION = 0.25 # Quarter Kelly
    MAX_BET_CAP = 0.05    # Hard cap at 5% of current bankroll
    
    bets_placed = 0
    winners_hit = 0
    total_action = 0.0
    
    logging.info("\n=== INITIATING DYNAMIC KELLY EXECUTION ===")
    logging.info("Ruleset:")
    logging.info("- Filter: Block Ranks 1 & 2 (The Consensus Trap)")
    logging.info("- Target: Bet only #1 AI Picks in Divergence Zone (Rank >= 3)")
    logging.info("- Sizing: Quarter Kelly (Max 5% Bankroll Cap)")
    logging.info("-" * 55)
    
    races = test_df.groupby(['date', 'race_id'])
    
    for (date, race_id), race_data in races:
        # Strict Oracle Rule: Only evaluate the AI's #1 pick
        top_pick = race_data[race_data['model_rank'] == 1.0]
        if len(top_pick) == 0: continue
        
        horse = top_pick.iloc[0]
        
        # THE FILTER: Starve the bad bets
        if horse['public_rank'] < 3.0:
            continue
            
        # THE MATH: Kelly Edge Calculation
        p = horse['model_prob']
        b = horse['win_odds'] - 1.0 # Net decimal odds
        q = 1.0 - p
        
        if b <= 0: continue
        
        # Calculate full Kelly fraction
        f_star = p - (q / b)
        
        # Only bet if the Mathematical Expected Value is positive
        if f_star > 0:
            # Apply fractional smoothing
            bet_pct = f_star * KELLY_FRACTION
            
            # Apply Risk Management Cap
            bet_pct = min(bet_pct, MAX_BET_CAP)
            
            wager = bankroll * bet_pct
            
            # Execute Trade
            bankroll -= wager
            total_action += wager
            bets_placed += 1
            
            # Settlement
            if horse['plc_num'] == 1.0:
                payout = wager * horse['win_odds']
                bankroll += payout
                winners_hit += 1
                
    # 5. Final Trading Desk Debrief
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
    run_sector_kelly_desk()