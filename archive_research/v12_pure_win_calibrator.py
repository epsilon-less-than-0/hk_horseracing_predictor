import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging
from sklearn.linear_model import LogisticRegression

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

class WinProbabilityCalibrator:
    """
    Direct Logit Calibration using Platt Scaling (Logistic Regression).
    Maps raw XGBRanker logits directly to base probabilities.
    """
    def __init__(self):
        self.calibrator = LogisticRegression(solver='lbfgs')
        
    def fit(self, validation_df: pd.DataFrame, score_col: str = 'model_score', target_col: str = 'is_win'):
        logging.info("Fitting Platt Scaling directly on raw XGBRanker logits...")
        X_val = validation_df[score_col].values.reshape(-1, 1)
        y_val = validation_df[target_col].values
        self.calibrator.fit(X_val, y_val)
        
    def transform(self, test_df: pd.DataFrame, score_col: str = 'model_score') -> pd.DataFrame:
        X_test = test_df[score_col].values.reshape(-1, 1)
        test_df['raw_calibrated_prob'] = self.calibrator.predict_proba(X_test)[:, 1]
        
        # Enforce Pari-Mutuel Closed System (Sum of probabilities in a race = 1.0)
        test_df['calibrated_win_prob'] = test_df['raw_calibrated_prob'] / test_df.groupby('race_id')['raw_calibrated_prob'].transform('sum')
        test_df = test_df.drop(columns=['raw_calibrated_prob'])
        return test_df

def train_ranker(train_df, features):
    logging.info(f"Training XGBRanker Oracle on {len(train_df['race_id'].unique())} historical races...")
    
    # Chronological sort required to prevent Look-Ahead bias inside the tree builder
    train_df = train_df.sort_values('race_id')
    
    X_train = train_df[features]
    # Invert finish position so 1 is mathematically the highest score
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

def run_win_calibrator_backtest():
    logging.info("=== PHASE 32: ALPHA MANIFOLD SWEEP (V10 VOLATILITY MATRIX) ===")
    
    # 1. Connect to the Vault - PULLING FROM V10
    conn = sqlite3.connect('hk_racing.db')
    df = pd.read_sql("SELECT * FROM ml_features_v10", conn)
    conn.close()

    # Rename target column to match the Execution Desk's expectations
    if 'plc' in df.columns:
        df.rename(columns={'plc': 'finish_position'}, inplace=True)

    # 2. Sanitize Keys
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['race_id'] = pd.to_numeric(df['race_id'], errors='coerce')
    df['horse_id'] = df['horse_id'].astype(str).str.strip()

    df.dropna(subset=['date', 'race_id', 'horse_id'], inplace=True)

    # 3. Clean Data Types and Create Binary Win Target
    df['finish_position'] = df['finish_position'].astype(str).str.extract(r'(\d+)').astype(float)
    
    if 'public_odds' in df.columns:
        df['public_odds'] = pd.to_numeric(df['public_odds'], errors='coerce')
    else:
        logging.error("FATAL: 'public_odds' column is missing from ml_features_v10.")
        return

    df['is_win'] = (df['finish_position'] == 1).astype(int)
    df.dropna(subset=['public_odds', 'finish_position'], inplace=True)

    # 4. Define Training Features - STRICT LEAKAGE PREVENTION
    exclude_metadata = [
        'date', 'race_id', 'horse_id', 'finish_position', 'public_odds', 
        'dividend', 'Race_No', 'plc', 'is_win', 'source_file', 'race_name',
        'going', 'race_type', 'race_dist', 'horse_name', 'jockey_name', 
        'trainer_name', 'running_pos', 'finish_time', 'lbw', 'parsed_lbw',
        'raw_ESI', 'raw_CSI'  # Quarantined Omniscience Leak
    ]
    base_features = [col for col in df.columns if col not in exclude_metadata]
    features = df[base_features].select_dtypes(include=[np.number, bool]).columns.tolist()
    
    logging.info(f"Matrix Sanity Check: Training on {len(features)} strictly shifted features.")
    logging.info(f"Active Features Detected: {', '.join(features)}")
    
    # 5. Temporal Split
    train_df = df[df['date'] < '2022-01-01'].copy()
    calib_df = df[(df['date'] >= '2022-01-01') & (df['date'] < '2023-01-01')].copy()
    test_df = df[df['date'] >= '2023-01-01'].copy()

    if len(train_df) == 0 or len(calib_df) == 0 or len(test_df) == 0:
        logging.error("FATAL: Temporal split resulted in an empty dataframe.")
        return

    # 6. Pipeline Execution
    ranker = train_ranker(train_df, features)
    
    calib_df['model_score'] = ranker.predict(calib_df[features])
    test_df['model_score'] = ranker.predict(test_df[features])
    
    calib_df['model_rank'] = calib_df.groupby('race_id')['model_score'].rank(ascending=False, method='dense')
    test_df['model_rank'] = test_df.groupby('race_id')['model_score'].rank(ascending=False, method='dense')

    calibrator = WinProbabilityCalibrator()
    calibrator.fit(calib_df, score_col='model_score', target_col='is_win')
    test_df = calibrator.transform(test_df, score_col='model_score')

    # --- 7. MASTER EXECUTION DESK (THE ALPHA SWEEPER) ---
    logging.info(f"\nExecuting Phase 32 Alpha Sweep on {len(test_df['race_id'].unique())} out-of-sample races...")
    
    # Calculate Public Implied Probability and Alpha Delta
    test_df['implied_prob'] = 1.0 / test_df['public_odds']
    test_df['alpha_delta'] = test_df['calibrated_win_prob'] - test_df['implied_prob']
    
    FLAT_BET_SIZE = 10.0
    MAX_ODDS = 15.0 # Keep the ceiling to filter structural noise
    
    # Define Grid Parameters - Expanding odds floors to hunt volatility
    delta_thresholds = [0.01, 0.02, 0.03, 0.04, 0.05]
    odds_floors = [3.0, 4.0, 5.0, 6.0] 
    
    results_log = []
    
    races = test_df.groupby('race_id')
    
    for min_odds in odds_floors:
        for delta_thresh in delta_thresholds:
            total_action = 0
            total_returned = 0
            winners = 0
            races_bet = 0
            
            for race_id, race_data in races:
                top_play = race_data[race_data['model_rank'] == 1.0]
                if top_play.empty: continue
                top_play = top_play.iloc[0]
                
                # Execution Logic
                if (min_odds <= top_play['public_odds'] <= MAX_ODDS) and (top_play['alpha_delta'] >= delta_thresh):
                    total_action += FLAT_BET_SIZE
                    races_bet += 1
                    
                    if top_play['is_win'] == 1:
                        winners += 1
                        total_returned += (FLAT_BET_SIZE * top_play['public_odds'])
                        
            # Accounting
            if races_bet > 0:
                roi = ((total_returned - total_action) / total_action) * 100
                hit_rate = (winners / races_bet) * 100
                results_log.append({
                    'Min_Odds': min_odds,
                    'Min_Delta': delta_thresh,
                    'Action_Volume': races_bet,
                    'Hit_Rate_%': hit_rate,
                    'Total_Return': total_returned - total_action,
                    'ROI_%': roi
                })

    # 8. Print Alpha Manifold Results
    logging.info("\n=== ALPHA MANIFOLD GRID SEARCH RESULTS (V10) ===")
    results_df = pd.DataFrame(results_log)
    if not results_df.empty:
        # Sort by Total Absolute Return (Profit)
        results_df = results_df.sort_values(by='Total_Return', ascending=False)
        print(results_df.to_string(index=False, float_format="%.2f"))
    else:
        logging.warning("No configurations yielded execution volume.")

if __name__ == "__main__":
    run_win_calibrator_backtest()