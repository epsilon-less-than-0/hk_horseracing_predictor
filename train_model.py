import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def train_xgboost_ranker_v2():
    logging.info("=== PHASE 5: TRAINING XGBOOST RANKER (V2.0 - Alpha Matrix) ===")
    
    # 1. Load Data
    conn = sqlite3.connect('hk_racing.db')
    logging.info("Loading V2.0 feature matrix...")
    df = pd.read_sql("SELECT * FROM ml_features", conn)
    conn.close()
    
    # 2. Data Cleaning & Target Engineering
    df['date'] = pd.to_datetime(df['date'])
    df['plc_num'] = pd.to_numeric(df['plc'], errors='coerce')
    
    df = df.dropna(subset=['plc_num'])
    
    # Relevance Target (15 - finish position)
    df['relevance'] = 15 - df['plc_num']
    df['relevance'] = df['relevance'].clip(lower=0)
    
    # NEW: Updated Feature List including Human/Geometric Elements
    features = [
        'pre_race_elo', 'days_since_last_run', 'weight_delta', 
        'distance_delta', 'career_wins', 'is_turf', 
        'draw', 'jockey_win_pct', 'trainer_win_pct'
    ]
    
    for col in features:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
        
    df = df.dropna(subset=features)
    
    # Intra-Race Sorting
    df = df.sort_values(by=['date', 'race_id', 'relevance'], ascending=[True, True, False])
    
    # 3. Chronological Train / Val / Test Split
    train_df = df[df['date'].dt.year <= 2023].copy()
    val_df   = df[df['date'].dt.year == 2024].copy()
    test_df  = df[df['date'].dt.year >= 2025].copy()
    
    logging.info(f"Split Sizes -> Train: {len(train_df):,}, Val: {len(val_df):,}, Test: {len(test_df):,}")
    
    if len(train_df) == 0:
        logging.error("Training data is empty! Stopping execution.")
        return
    
    X_train, y_train = train_df[features], train_df['relevance']
    X_val, y_val     = val_df[features], val_df['relevance']
    
    group_train = train_df.groupby(['date', 'race_id'], sort=False).size().values
    group_val   = val_df.groupby(['date', 'race_id'], sort=False).size().values
    
    # 4. Initialize and Train XGBoost Ranker
    logging.info("Initializing XGBRanker (Objective: rank:pairwise)...")
    
    ranker = xgb.XGBRanker(
        tree_method='hist',
        objective='rank:pairwise',
        eval_metric='ndcg',
        learning_rate=0.01,
        max_depth=5,
        subsample=0.8,
        colsample_bytree=0.5,  # Crucial: Forces the model to look at the new Jockey/Trainer features
        n_estimators=1000,
        early_stopping_rounds=100
    )
    
    logging.info("Training model...")
    ranker.fit(
        X_train, y_train,
        group=group_train,
        eval_set=[(X_val, y_val)],
        eval_group=[group_val],
        verbose=50
    )
    
    logging.info("\n=== TRAINING COMPLETE ===")
    
    # 5. Feature Importance
    importance = ranker.feature_importances_
    logging.info("\n[FEATURE IMPORTANCE]")
    for i, col in enumerate(features):
        logging.info(f"{col}: {importance[i]:.4f}")
        
    # Save the V2.0 model
    ranker.save_model('v2_hkjc_ranker.json')
    logging.info("\nModel saved to 'v2_hkjc_ranker.json'. Ready for backtesting.")

if __name__ == "__main__":
    train_xgboost_ranker_v2()