import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def train_ranker_and_extract(train_df, features):
    logging.info(f"Training XGBRanker Oracle on {len(train_df['race_id'].unique())} historical races...")
    train_df = train_df.sort_values('race_id')
    
    X_train = train_df[features]
    y_train = 20 - train_df['finish_position'].fillna(20)
    groups_train = train_df.groupby('race_id').size().values
    
    ranker = xgb.XGBRanker(
        tree_method='hist',
        objective='rank:pairwise',
        learning_rate=0.05,
        max_depth=4,
        colsample_bytree=0.5,
        n_estimators=150,
        random_state=42
    )
    
    ranker.fit(X_train, y_train, group=groups_train)
    
    # --- PHASE 21: FEATURE AUTOPSY ---
    importance_df = pd.DataFrame({
        'Feature': features,
        'Importance_Weight': ranker.feature_importances_
    }).sort_values(by='Importance_Weight', ascending=False)
    
    logging.info("\n=== THE ALPHA LEDGER: TOP 25 PREDICTIVE FEATURES ===")
    logging.info(f"{'Rank':<5} | {'Feature Name':<35} | {'Weight'}")
    logging.info("-" * 60)
    
    for i, (idx, row) in enumerate(importance_df.head(25).iterrows(), 1):
        logging.info(f"#{i:<4} | {row['Feature']:<35} | {row['Importance_Weight']:.4f}")
        
    return ranker

def run_feature_extraction():
    logging.info("=== PHASE 21: ALPHA EXTRACTION (LEAK PLUGGED) ===")
    
    # 1. Connect to the Vault
    conn = sqlite3.connect('hk_racing.db')
    df_features = pd.read_sql("SELECT * FROM ml_features_v7", conn)
    df_odds = pd.read_sql("SELECT date, race_id, horse_id, plc FROM race_results", conn)
    df_odds.rename(columns={'plc': 'finish_position'}, inplace=True)
    conn.close()

    # 2. Sanitize Data
    df_features['date'] = pd.to_datetime(df_features['date'], errors='coerce')
    df_odds['date'] = pd.to_datetime(df_odds['date'], errors='coerce')
    df_features['race_id'] = pd.to_numeric(df_features['race_id'], errors='coerce')
    df_odds['race_id'] = pd.to_numeric(df_odds['race_id'], errors='coerce')
    df_features['horse_id'] = df_features['horse_id'].astype(str).str.strip()
    df_odds['horse_id'] = df_odds['horse_id'].astype(str).str.strip()

    df_features.dropna(subset=['date', 'race_id', 'horse_id'], inplace=True)
    df_odds.dropna(subset=['date', 'race_id', 'horse_id'], inplace=True)

    # 3. Merge
    df = pd.merge(df_features, df_odds, on=['date', 'race_id', 'horse_id'])
    df['finish_position'] = df['finish_position'].astype(str).str.extract(r'(\d+)').astype(float)

    # 4. Define Features (THE TARGET LEAK IS PLUGGED HERE)
    exclude_metadata = ['date', 'race_id', 'horse_id', 'finish_position', 'public_odds', 'dividend', 'Race_No', 'plc']
    base_features = [col for col in df.columns if col not in exclude_metadata]
    features = df[base_features].select_dtypes(include=[np.number, bool]).columns.tolist()

    # 5. Extract purely from the Training set to avoid data leakage
    train_df = df[df['date'] < '2023-01-01'].copy()
    
    if len(train_df) == 0:
        logging.error("FATAL: Training dataset is empty.")
        return
        
    train_ranker_and_extract(train_df, features)

if __name__ == "__main__":
    run_feature_extraction()