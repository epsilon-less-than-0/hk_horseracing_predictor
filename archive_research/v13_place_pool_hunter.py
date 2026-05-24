import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging
from sklearn.linear_model import LogisticRegression

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

class PlaceProbabilityCalibrator:
    """
    Direct Logit Calibration for the PLACE POOL.
    Maps raw XGBRanker logits to P(Top 3 Finish).
    """
    def __init__(self):
        self.calibrator = LogisticRegression(solver='lbfgs', max_iter=500)
        
    def fit(self, validation_df: pd.DataFrame, score_col: str = 'model_score', target_col: str = 'is_place'):
        logging.info("Fitting Platt Scaling for P(Place) on raw XGBRanker logits...")
        X_val = validation_df[score_col].values.reshape(-1, 1)
        y_val = validation_df[target_col].values
        self.calibrator.fit(X_val, y_val)
        
    def transform(self, test_df: pd.DataFrame, score_col: str = 'model_score') -> pd.DataFrame:
        X_test = test_df[score_col].values.reshape(-1, 1)
        # We don't normalize to 1.0 here because a race has 3 place positions, not 1.
        test_df['calibrated_place_prob'] = self.calibrator.predict_proba(X_test)[:, 1]
        return test_df

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

def run_place_pool_backtest():
    logging.info("=== PHASE 33: THE PLACE POOL VOLATILITY HUNTER (V10 MATRIX) ===")
    
    # 1. Connect to Vault
    conn = sqlite3.connect('hk_racing.db')
    df = pd.read_sql("SELECT * FROM ml_features_v10", conn)
    conn.close()

    if 'plc' in df.columns:
        df.rename(columns={'plc': 'finish_position'}, inplace=True)

    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['race_id'] = pd.to_numeric(df['race_id'], errors='coerce')
    df['horse_id'] = df['horse_id'].astype(str).str.strip()
    df.dropna(subset=['date', 'race_id', 'horse_id'], inplace=True)

    # 3. Target Re-Engineering: The Volatility Pivot
    df['finish_position'] = df['finish_position'].astype(str).str.extract(r'(\d+)').astype(float)
    df['public_odds'] = pd.to_numeric(df['public_odds'], errors='coerce')
    
    # NEW TARGET: Top 3 Finish
    df['is_place'] = (df['finish_position'] <= 3).astype(int)
    df.dropna(subset=['public_odds', 'finish_position'], inplace=True)

    # 4. Strict Leakage Prevention
    exclude_metadata = [
        'date', 'race_id', 'horse_id', 'finish_position', 'public_odds', 
        'dividend', 'Race_No', 'plc', 'is_win', 'is_place', 'source_file', 'race_name',
        'going', 'race_type', 'race_dist', 'horse_name', 'jockey_name', 
        'trainer_name', 'running_pos', 'finish_time', 'lbw', 'parsed_lbw',
        'raw_ESI', 'raw_CSI'
    ]
    base_features = [col for col in df.columns if col not in exclude_metadata]
    features = df[base_features].select_dtypes(include=[np.number, bool]).columns.tolist()
    
    # 5. Temporal Split
    train_df = df[df['date'] < '2022-01-01'].copy()
    calib_df = df[(df['date'] >= '2022-01-01') & (df['date'] < '2023-01-01')].copy()
    test_df = df[df['date'] >= '2023-01-01'].copy()

    # 6. Pipeline Execution
    ranker = train_ranker(train_df, features)
    
    calib_df['model_score'] = ranker.predict(calib_df[features])
    test_df['model_score'] = ranker.predict(test_df[features])
    
    # Rank remains the same, but the Calibrator maps it to P(Place)
    calib_df['model_rank'] = calib_df.groupby('race_id')['model_score'].rank(ascending=False, method='dense')
    test_df['model_rank'] = test_df.groupby('race_id')['model_score'].rank(ascending=False, method='dense')

    calibrator = PlaceProbabilityCalibrator()
    calibrator.fit(calib_df, score_col='model_score', target_col='is_place')
    test_df = calibrator.transform(test_df, score_col='model_score')

    # --- 7. PLACE POOL ALPHA SWEEPER ---
    logging.info(f"\nExecuting Phase 33 Place Pool Sweep on {len(test_df['race_id'].unique())} out-of-sample races...")
    
    # We hunt value: P(Place) MUST be exceptionally high, and Win Odds must be attractive.
    place_prob_thresholds = [0.35, 0.40, 0.45, 0.50]
    odds_floors = [5.0, 7.0, 10.0] 
    
    results_log = []
    
    for min_odds in odds_floors:
        for min_place_prob in place_prob_thresholds:
            places_hit = 0
            races_bet = 0
            
            # Vectorized execution for speed
            executions = test_df[
                (test_df['public_odds'] >= min_odds) & 
                (test_df['calibrated_place_prob'] >= min_place_prob) &
                (test_df['model_rank'] <= 3.0) # Ensure it's a top pick
            ]
            
            races_bet = len(executions)
            places_hit = executions['is_place'].sum()
                        
            if races_bet > 0:
                hit_rate = (places_hit / races_bet) * 100
                results_log.append({
                    'Win_Odds_Floor': min_odds,
                    'Min_P(Place)': min_place_prob,
                    'Action_Volume': races_bet,
                    'Top_3_Hit_Rate_%': hit_rate
                })

    logging.info("\n=== PLACE POOL VOLATILITY GRID RESULTS ===")
    results_df = pd.DataFrame(results_log)
    if not results_df.empty:
        results_df = results_df.sort_values(by='Action_Volume', ascending=False)
        print(results_df.to_string(index=False, float_format="%.2f"))
    else:
        logging.warning("No configurations yielded execution volume.")

if __name__ == "__main__":
    run_place_pool_backtest()