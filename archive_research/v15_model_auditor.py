import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import shap
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

def train_ranker(train_df, features):
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
    return ranker

def run_shap_audit():
    logging.info("=== PHASE 37: SHAP VALUE MANIFOLD AUDIT (V12 MATRIX) ===")
    
    # 1. Connect to Vault
    conn = sqlite3.connect('hk_racing.db')
    df = pd.read_sql("SELECT * FROM ml_features_v12", conn)
    conn.close()

    if 'plc' in df.columns:
        df.rename(columns={'plc': 'finish_position'}, inplace=True)

    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['race_id'] = pd.to_numeric(df['race_id'], errors='coerce')
    df['horse_id'] = df['horse_id'].astype(str).str.strip()
    df.dropna(subset=['date', 'race_id', 'horse_id'], inplace=True)

    df['finish_position'] = df['finish_position'].astype(str).str.extract(r'(\d+)').astype(float)
    df.dropna(subset=['finish_position'], inplace=True)

    # 2. Strict Leakage Prevention 
    exclude_metadata = [
        'date', 'race_id', 'horse_id', 'finish_position', 'public_odds', 
        'dividend', 'Race_No', 'plc', 'is_win', 'is_place', 'is_quinella_hit', 
        'source_file', 'race_name', 'going', 'race_type', 'race_dist', 
        'horse_name', 'jockey_name', 'trainer_name', 'running_pos', 
        'finish_time', 'lbw', 'parsed_lbw', 'raw_ESI', 'raw_CSI'
    ]
    base_features = [col for col in df.columns if col not in exclude_metadata]
    features = df[base_features].select_dtypes(include=[np.number, bool]).columns.tolist()
    
    train_df = df[df['date'] < '2022-01-01'].copy()
    test_df = df[df['date'] >= '2023-01-01'].copy()

    if len(train_df) == 0 or len(test_df) == 0:
        logging.error("FATAL: Temporal split resulted in an empty dataframe.")
        return

    # 3. Train Model
    ranker = train_ranker(train_df, features)
    
    # 4. SHAP Computation Pipeline (Optimized for Raw Booster)
    logging.info("\nExtracting Raw Booster and binding feature names...")
    booster = ranker.get_booster()
    booster.feature_names = features
    
    logging.info("Calculating SHAP Values on Out-of-Sample Matrix (This may take a moment)...")
    X_test = test_df[features]
    
    # Explainer strictly using the raw booster
    explainer = shap.TreeExplainer(booster)
    shap_values = explainer.shap_values(X_test)
    
    # Calculate Mean Absolute SHAP values across all test predictions
    mean_abs_shap = np.abs(shap_values).mean(axis=0)
    
    # Compile results
    shap_df = pd.DataFrame({
        'Feature': features,
        'Mean_Absolute_SHAP': mean_abs_shap
    })
    shap_df = shap_df.sort_values(by='Mean_Absolute_SHAP', ascending=False).reset_index(drop=True)
    
    # Calculate relative importance
    total_shap = shap_df['Mean_Absolute_SHAP'].sum()
    shap_df['Relative_Impact_%'] = (shap_df['Mean_Absolute_SHAP'] / total_shap) * 100

    logging.info("\n=== GLOBAL FEATURE IMPORTANCE (SHAP AUDIT) ===")
    logging.info("Ranking the Orthogonal Alpha drivers for the V12 Engine:\n")
    print(shap_df.to_string(index=True, float_format="%.4f"))
    
    logging.info("\nNOTE: Features with < 2.0% impact are candidates for pruning.")

if __name__ == "__main__":
    run_shap_audit()