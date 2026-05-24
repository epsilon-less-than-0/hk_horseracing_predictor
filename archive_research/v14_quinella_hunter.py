import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging
from sklearn.linear_model import LogisticRegression

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

class PlaceProbabilityCalibrator:
    def __init__(self):
        self.calibrator = LogisticRegression(solver='lbfgs', max_iter=500)
        
    def fit(self, validation_df: pd.DataFrame, score_col: str = 'model_score', target_col: str = 'is_place'):
        logging.info("Fitting Platt Scaling for P(Place) on raw XGBRanker logits...")
        X_val = validation_df[score_col].values.reshape(-1, 1)
        y_val = validation_df[target_col].values
        self.calibrator.fit(X_val, y_val)
        
    def transform(self, test_df: pd.DataFrame, score_col: str = 'model_score') -> pd.DataFrame:
        X_test = test_df[score_col].values.reshape(-1, 1)
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

def run_quinella_backtest():
    logging.info("=== PHASE 36: EXOTIC BANKER SYNTHESIS (V12 TDA MATRIX) ===")
    
    # 1. Connect to Vault - PULLING FROM V12
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
    df['public_odds'] = pd.to_numeric(df['public_odds'], errors='coerce')
    
    # Targets
    df['is_place'] = (df['finish_position'] <= 3).astype(int)
    df['is_quinella_hit'] = (df['finish_position'] <= 2).astype(int)
    df.dropna(subset=['public_odds', 'finish_position'], inplace=True)

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
    
    logging.info(f"Matrix Sanity Check: Training on {len(features)} strictly shifted features.")
    
    train_df = df[df['date'] < '2022-01-01'].copy()
    calib_df = df[(df['date'] >= '2022-01-01') & (df['date'] < '2023-01-01')].copy()
    test_df = df[df['date'] >= '2023-01-01'].copy()

    if len(train_df) == 0 or len(test_df) == 0:
        logging.error("FATAL: Temporal split resulted in an empty dataframe.")
        return

    # 3. Pipeline Execution
    ranker = train_ranker(train_df, features)
    
    calib_df['model_score'] = ranker.predict(calib_df[features])
    test_df['model_score'] = ranker.predict(test_df[features])
    
    calibrator = PlaceProbabilityCalibrator()
    calibrator.fit(calib_df, score_col='model_score', target_col='is_place')
    test_df = calibrator.transform(test_df, score_col='model_score')

    # --- 4. THE QUINELLA EXECUTION DESK ---
    logging.info(f"\nExecuting Phase 36 Quinella Simulator on {len(test_df['race_id'].unique())} races...")
    
    # Hunting for Orthogonal Volatility Plays
    MIN_BANKER_ODDS = 7.0
    MIN_PLACE_PROB = 0.40
    BASE_BET = 10.0
    
    total_action = 0
    quinellas_hit = 0
    races_bet = 0
    
    races = test_df.groupby('race_id')
    
    for race_id, race_data in races:
        # Identify Alpha Banker
        potential_bankers = race_data[
            (race_data['public_odds'] >= MIN_BANKER_ODDS) & 
            (race_data['calibrated_place_prob'] >= MIN_PLACE_PROB)
        ]
        
        if potential_bankers.empty:
            continue
            
        banker = potential_bankers.sort_values(by='calibrated_place_prob', ascending=False).iloc[0]
        
        # Identify Consensus Legs (Top 2 public favorites)
        legs = race_data[race_data['horse_id'] != banker['horse_id']].sort_values(by='public_odds', ascending=True).head(2)
        
        if len(legs) < 2:
            continue
            
        ticket_cost = BASE_BET * 2
        total_action += ticket_cost
        races_bet += 1
        
        banker_hit = banker['is_quinella_hit'] == 1
        legs_hit = legs['is_quinella_hit'].sum() >= 1
        
        if banker_hit and legs_hit:
            quinellas_hit += 1

    # 5. Synthesized Ledger
    if races_bet > 0:
        hit_rate = (quinellas_hit / races_bet) * 100
        
        logging.info("\n=== THE QUINELLA BANKER LEDGER (V12) ===")
        logging.info(f"Banker Criteria: Odds >= {MIN_BANKER_ODDS}, P(Place) >= {MIN_PLACE_PROB}")
        logging.info(f"Ticket Structure: 1 Banker > 2 Public Favorites (Cost: ${BASE_BET * 2:.2f})")
        logging.info("-" * 40)
        logging.info(f"Total Eligible Races: {len(test_df['race_id'].unique()):,}")
        logging.info(f"Races Bet (Action Taken): {races_bet:,}")
        logging.info(f"Winning Quinellas: {quinellas_hit:,}")
        logging.info(f"Quinella Hit Rate: {hit_rate:.2f}%")
        logging.info(f"Total Capital Risked: ${total_action:,.2f}")
        logging.info("-" * 40)
        
        avg_required_dividend = total_action / quinellas_hit if quinellas_hit > 0 else 0
        logging.info(f"\n>> Required Average Dividend to Break Even: ${avg_required_dividend:.2f}")

    else:
        logging.warning("\nNo Quinella tickets executed based on the current thresholds.")

if __name__ == "__main__":
    run_quinella_backtest()