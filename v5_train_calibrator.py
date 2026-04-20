import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging
from sklearn.metrics import log_loss, brier_score_loss

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def train_probability_calibrator():
    logging.info("=== PHASE 10: TRAINING THE META-MODEL CALIBRATOR (V5.0) ===")
    
    # 1. Load the V3 Ranker
    try:
        ranker = xgb.XGBRanker()
        ranker.load_model('v3_hkjc_ranker.json')
        logging.info("Loaded V3 Targeter (Ranker).")
    except Exception as e:
        logging.error("Could not load V3 model. Ensure 'v3_hkjc_ranker.json' exists.")
        return

    # 2. Extract Data from the Vault
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
    
    # The Ranker's exact feature set
    ranker_features = [
        'pre_race_elo', 'days_since_last_run', 'weight_delta', 
        'distance_delta', 'career_wins', 'is_turf', 
        'draw', 'jockey_win_pct', 'trainer_win_pct',
        'shifted_rolling_ESI', 'shifted_rolling_CSI', 
        'race_ESI_pressure', 'pace_advantage'
    ]

    # Clean the dataset
    df = df.dropna(subset=['plc_num', 'win_odds'] + ranker_features).copy()
    
    # Generate the Public Implied Probability (The Wisdom of the Crowds)
    df['public_implied_prob'] = 1.0 / df['win_odds']
    
    # Generate the Binary Target (Did the horse win?)
    df['is_win'] = (df['plc_num'] == 1.0).astype(int)

    # 3. Isolate the 2024 Calibration Set
    # We strictly use 2024 to train the calibrator, leaving 2025+ totally unseen for the backtest
    calib_df = df[df['date'].dt.year == 2024].sort_values(by=['date', 'race_id']).copy()
    
    if len(calib_df) == 0:
        logging.error("Calibration data (2024) is empty!")
        return
        
    logging.info(f"Extracting Ranker Scores for {len(calib_df)} horses in the 2024 Calibration Set...")
    X_ranker = calib_df[ranker_features].astype(float)
    calib_df['ranker_raw_score'] = ranker.predict(X_ranker)

    # 4. Train the Logistic Calibrator
    # We feed it the AI's opinion, the Public's opinion, and the physical Pace Advantage
    calibrator_features = ['ranker_raw_score', 'public_implied_prob', 'pace_advantage']
    X_calib = calib_df[calibrator_features]
    y_calib = calib_df['is_win']

    logging.info("Initializing XGBClassifier (Objective: binary:logistic)...")
    calibrator = xgb.XGBClassifier(
        objective='binary:logistic',
        eval_metric='logloss',
        learning_rate=0.05,
        max_depth=3, # Keep it shallow to prevent overfitting
        n_estimators=300
    )

    logging.info("Training Meta-Model...")
    calibrator.fit(X_calib, y_calib)

    # 5. Evaluate Calibration Quality
    calib_df['calibrated_prob'] = calibrator.predict_proba(X_calib)[:, 1]
    
    brier_score = brier_score_loss(y_calib, calib_df['calibrated_prob'])
    logloss = log_loss(y_calib, calib_df['calibrated_prob'])
    
    logging.info("\n=== CALIBRATION METRICS ===")
    logging.info(f"Brier Score: {brier_score:.4f} (Lower is better, closer to true probability)")
    logging.info(f"Log Loss:    {logloss:.4f}")
    
    importance = calibrator.feature_importances_
    logging.info("\n[CALIBRATOR FEATURE IMPORTANCE]")
    for i, col in enumerate(calibrator_features):
        logging.info(f"{col}: {importance[i]:.4f}")

    # 6. Save the Calibrator
    calibrator.save_model('v5_hkjc_calibrator.json')
    logging.info("\nMeta-Model saved to 'v5_hkjc_calibrator.json'. The Execution Desk is ready.")

if __name__ == "__main__":
    train_probability_calibrator()