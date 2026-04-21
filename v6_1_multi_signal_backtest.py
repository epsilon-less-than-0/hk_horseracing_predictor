import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging
from sklearn.metrics import brier_score_loss

logging.basicConfig(level=logging.INFO, format='%(message)s')

def run_multi_signal_backtest():
    logging.info("=== PHASE 12.1: MULTI-SIGNAL MACRO BACKTEST (V6.1) ===")
    logging.info("Aperture Widened: Evaluating Top 3 AI Picks per race.")
    
    # 1. Extract Data
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
    df['relevance'] = (15 - df['plc_num']).clip(lower=0)

    features = [
        'pre_race_elo', 'days_since_last_run', 'weight_delta', 
        'distance_delta', 'career_wins', 'is_turf', 
        'draw', 'jockey_win_pct', 'trainer_win_pct',
        'shifted_rolling_ESI', 'shifted_rolling_CSI', 
        'race_ESI_pressure', 'pace_advantage'
    ]

    df = df.dropna(subset=['plc_num', 'win_odds'] + features).copy()
    df = df.sort_values(by=['date', 'race_id', 'relevance'], ascending=[True, True, False])

    # 2. The Macro Splits
    train_df = df[df['date'].dt.year <= 2021].copy()
    calib_df = df[df['date'].dt.year == 2022].copy()
    test_df  = df[df['date'].dt.year >= 2023].copy()

    # 3. Stage 1: Train the Targeter (Ranker)
    logging.info("\nStage 1: Training XGBRanker (2018-2021)...")
    X_train, y_train = train_df[features], train_df['relevance']
    group_train = train_df.groupby(['date', 'race_id'], sort=False).size().values
    
    ranker = xgb.XGBRanker(
        tree_method='hist', objective='rank:pairwise', eval_metric='ndcg',
        learning_rate=0.01, max_depth=5, subsample=0.8, colsample_bytree=0.5, n_estimators=1000
    )
    ranker.fit(X_train, y_train, group=group_train, verbose=False)

    # 4. Stage 2: Train the Calibrator
    logging.info("Stage 2: Training Meta-Model Calibrator (2022)...")
    calib_df['public_implied_prob'] = 1.0 / calib_df['win_odds']
    calib_df['is_win'] = (calib_df['plc_num'] == 1.0).astype(int)
    calib_df['ranker_raw_score'] = ranker.predict(calib_df[features].astype(float))

    calibrator_features = ['ranker_raw_score', 'public_implied_prob', 'pace_advantage']
    X_calib = calib_df[calibrator_features]
    y_calib = calib_df['is_win']

    calibrator = xgb.XGBClassifier(
        objective='binary:logistic', eval_metric='logloss', learning_rate=0.05, max_depth=3, n_estimators=300
    )
    calibrator.fit(X_calib, y_calib)

    # 5. Stage 3: Multi-Signal Execution Desk
    logging.info("Stage 3: Running Multi-Signal Kelly Desk (2023-2025)...")
    test_df['ranker_raw_score'] = ranker.predict(test_df[features].astype(float))
    test_df['model_rank'] = test_df.groupby(['date', 'race_id'])['ranker_raw_score'].rank(ascending=False, method='min')
    test_df['public_rank'] = test_df.groupby(['date', 'race_id'])['win_odds'].rank(ascending=True, method='min')
    
    test_df['public_implied_prob'] = 1.0 / test_df['win_odds']
    test_df['calibrated_prob'] = calibrator.predict_proba(test_df[calibrator_features])[:, 1]

    STARTING_BANKROLL = 10000.0
    bankroll = STARTING_BANKROLL
    KELLY_FRACTION = 0.25
    MAX_RACE_CAP = 0.05 # Max 5% of bankroll risked per ENTIRE race
    
    bets_placed = 0
    winners_hit = 0
    total_action = 0.0
    
    races = test_df.groupby(['date', 'race_id'])
    
    for (date, race_id), race_data in races:
        # NEW LOGIC: Evaluate the Top 3 AI picks, not just the #1 pick
        top_picks = race_data[race_data['model_rank'] <= 3.0]
        if len(top_picks) == 0: continue
        
        race_risk_total = 0.0
        race_payout = 0.0
        
        for _, horse in top_picks.iterrows():
            # Still block the Consensus Trap (Favorites)
            if horse['public_rank'] < 3.0:
                continue
                
            p = horse['calibrated_prob']
            b = horse['win_odds'] - 1.0
            q = 1.0 - p
            
            if b <= 0: continue
            f_star = p - (q / b)
            
            if f_star > 0:
                # Calculate raw bet percentage
                bet_pct = f_star * KELLY_FRACTION
                
                # Enforce the strict Race Cap
                if (race_risk_total + bet_pct) > MAX_RACE_CAP:
                    bet_pct = MAX_RACE_CAP - race_risk_total
                
                if bet_pct <= 0: break # Race cap reached, no more bets
                
                wager = bankroll * bet_pct
                race_risk_total += bet_pct
                
                total_action += wager
                bets_placed += 1
                
                if horse['plc_num'] == 1.0:
                    race_payout += (wager * horse['win_odds'])
                    winners_hit += 1
                    
        # Settle the entire race at once
        if race_risk_total > 0:
            bankroll -= (bankroll * race_risk_total) # Deduct all wagers
            bankroll += race_payout                  # Add any winnings

    # 6. Macro Debrief
    roi = ((bankroll - STARTING_BANKROLL) / total_action) * 100 if total_action > 0 else 0
    win_rate = (winners_hit / bets_placed) * 100 if bets_placed > 0 else 0
    
    logging.info("\n=== MULTI-SIGNAL MACRO DEBRIEF ===")
    logging.info(f"Total Action (Vol): ${total_action:,.2f}")
    logging.info(f"Bets Executed:      {bets_placed}")
    logging.info(f"Winners Hit:        {winners_hit} (Win Rate: {win_rate:.1f}%)")
    logging.info(f"Starting Bankroll:  ${STARTING_BANKROLL:,.2f}")
    logging.info(f"Ending Bankroll:    ${bankroll:,.2f}")
    
    if bankroll > STARTING_BANKROLL:
        logging.info(f"Net Profit:         +${(bankroll - STARTING_BANKROLL):,.2f}")
        logging.info(f"Yield (ROI):        +{roi:.2f}%")
    else:
        logging.info(f"Net Loss:           -${(STARTING_BANKROLL - bankroll):,.2f}")
        logging.info(f"Yield (ROI):        {roi:.2f}%")

if __name__ == "__main__":
    run_multi_signal_backtest()