import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def train_ranker(train_df, features):
    logging.info(f"Training XGBRanker Oracle on {len(train_df['race_id'].unique())} historical races...")
    train_df = train_df.sort_values('race_id')
    
    X_train = train_df[features]
    # Target Inverted so 1st place is mathematically the highest score
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
    return ranker

def run_ground_truth_backtest():
    logging.info("=== PHASE 20: GROUND TRUTH ROI (V10.2 - Leak Plugged) ===")
    
    # 1. Connect to the Vault
    conn = sqlite3.connect('hk_racing.db')
    df_features = pd.read_sql("SELECT * FROM ml_features_v7", conn)
    df_odds = pd.read_sql("SELECT date, race_id, horse_id, public_odds, plc FROM race_results", conn)
    df_odds.rename(columns={'plc': 'finish_position'}, inplace=True)
    df_payouts = pd.read_sql("SELECT * FROM quinella_payouts", conn)
    conn.close()

    # 2. SANITIZE JOIN KEYS BEFORE MERGING
    df_features['date'] = pd.to_datetime(df_features['date'], errors='coerce')
    df_odds['date'] = pd.to_datetime(df_odds['date'], errors='coerce')
    
    df_features['race_id'] = pd.to_numeric(df_features['race_id'], errors='coerce')
    df_odds['race_id'] = pd.to_numeric(df_odds['race_id'], errors='coerce')
    df_payouts['race_id'] = pd.to_numeric(df_payouts['race_id'], errors='coerce')

    df_features['horse_id'] = df_features['horse_id'].astype(str).str.strip()
    df_odds['horse_id'] = df_odds['horse_id'].astype(str).str.strip()

    df_features.dropna(subset=['date', 'race_id', 'horse_id'], inplace=True)
    df_odds.dropna(subset=['date', 'race_id', 'horse_id'], inplace=True)
    df_payouts.dropna(subset=['race_id'], inplace=True)

    # 3. Merge Pipeline
    df = pd.merge(df_features, df_odds, on=['date', 'race_id', 'horse_id'])
    df = pd.merge(df, df_payouts, on='race_id', how='left') 
    
    # 4. Clean Data Types
    df['finish_position'] = df['finish_position'].astype(str).str.extract(r'(\d+)').astype(float)
    df['dividend'] = pd.to_numeric(df['dividend'], errors='coerce')

    # 5. Define training features (THE TARGET LEAK IS PLUGGED HERE)
    exclude_metadata = ['date', 'race_id', 'horse_id', 'finish_position', 'public_odds', 'dividend', 'Race_No', 'plc']
    base_features = [col for col in df.columns if col not in exclude_metadata]
    features = df[base_features].select_dtypes(include=[np.number, bool]).columns.tolist()

    # 6. Temporal Split
    train_df = df[df['date'] < '2023-01-01'].copy()
    test_df = df[df['date'] >= '2023-01-01'].copy()
    
    test_df = test_df.dropna(subset=['dividend'])

    # 7. Train the Model
    if len(train_df) == 0:
        logging.error("FATAL: Training dataset is empty.")
        return
        
    ranker = train_ranker(train_df, features)
    
    # 8. Generate Predictions
    if len(test_df) == 0:
        logging.error("FATAL: Testing dataset is empty.")
        return
        
    test_df['model_score'] = ranker.predict(test_df[features])
    test_df['model_rank'] = test_df.groupby('race_id')['model_score'].rank(ascending=False, method='dense')

    # --- 9. MASTER EXECUTION DESK ---
    logging.info(f"Executing Combinatorial Box Strategy on {len(test_df['race_id'].unique())} scraped races...")
    
    total_action = 0
    total_returned = 0
    winners = 0
    races_bet = 0
    
    races = test_df.groupby('race_id')
    
    for race_id, race_data in races:
        actual_1st = race_data[race_data['finish_position'] == 1]['horse_id'].values
        actual_2nd = race_data[race_data['finish_position'] == 2]['horse_id'].values
        
        if len(actual_1st) == 0 or len(actual_2nd) == 0:
            continue
            
        top_3 = race_data.nsmallest(3, 'model_rank')
        
        total_action += 30
        races_bet += 1
        
        is_1st_in_top3 = actual_1st[0] in top_3['horse_id'].values
        is_2nd_in_top3 = actual_2nd[0] in top_3['horse_id'].values
        
        if is_1st_in_top3 and is_2nd_in_top3:
            winners += 1
            total_returned += race_data['dividend'].iloc[0]

    # 10. Financial Accounting
    if races_bet > 0:
        roi = ((total_returned - total_action) / total_action) * 100
        win_rate = (winners / races_bet) * 100
        
        logging.info("\n=== FINAL LEDGER ===")
        logging.info(f"Races Evaluated: {races_bet:,}")
        logging.info(f"Winning Quinellas: {winners:,}")
        logging.info(f"Hit Rate: {win_rate:.2f}%")
        logging.info(f"Total Capital Risked: ${total_action:,}")
        logging.info(f"Total Capital Returned: ${total_returned:,.2f}")
        
        if roi > 0:
            logging.info(f"GROUND TRUTH ROI: +{roi:.2f}%  [PROFITABLE]")
        else:
            logging.info(f"GROUND TRUTH ROI: {roi:.2f}%  [LOSS]")

if __name__ == "__main__":
    run_ground_truth_backtest()