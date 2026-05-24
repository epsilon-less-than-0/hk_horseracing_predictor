import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

class PlaceProbabilityCalibrator:
    def __init__(self):
        self.calibrator = LogisticRegression(solver='lbfgs', max_iter=500)
    def fit(self, validation_df: pd.DataFrame, score_col: str = 'model_score', target_col: str = 'is_place'):
        X_val = validation_df[score_col].values.reshape(-1, 1)
        self.calibrator.fit(X_val, validation_df[target_col].values)
    def transform(self, test_df: pd.DataFrame, score_col: str = 'model_score') -> pd.DataFrame:
        X_test = test_df[score_col].values.reshape(-1, 1)
        test_df['calibrated_place_prob'] = self.calibrator.predict_proba(X_test)[:, 1]
        return test_df

def train_ranker(train_df, features):
    train_df = train_df.sort_values('race_id')
    ranker = xgb.XGBRanker(
        tree_method='hist', 
        objective='rank:pairwise', 
        learning_rate=0.05, 
        max_depth=4, 
        colsample_bytree=0.5, 
        n_estimators=150, 
        random_state=42
    )
    ranker.fit(train_df[features], 20 - train_df['finish_position'].fillna(20), group=train_df.groupby('race_id').size().values)
    return ranker

def run_regime_audit():
    logging.info("=== PHASE 43: STRUCTURAL REGIME STRESS DESK (V12 MATRIX) ===")
    
    conn = sqlite3.connect('hk_racing.db')
    df = pd.read_sql("SELECT * FROM ml_features_v12", conn)
    conn.close()

    df.rename(columns={'plc': 'finish_position'}, inplace=True)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['race_id'] = pd.to_numeric(df['race_id'], errors='coerce')
    df['horse_id'] = df['horse_id'].astype(str).str.strip()
    df.dropna(subset=['date', 'race_id', 'horse_id'], inplace=True)

    df['finish_position'] = df['finish_position'].astype(str).str.extract(r'(\d+)').astype(float)
    df['public_odds'] = pd.to_numeric(df['public_odds'], errors='coerce')
    df['is_place'] = (df['finish_position'] <= 3).astype(int)
    df.dropna(subset=['public_odds', 'finish_position', 'race_dist'], inplace=True)

    # Reconstruct Class boundaries based on weight/metadata or extract proxy
    # For this audit, we will use structural Distance Regimes as our block boundaries
    def map_distance_regime(dist_str):
        try:
            d = int(re.sub(r'\D', '', str(dist_str)))
            if d <= 1200: return 'Sprint (<=1200m)'
            elif d <= 1650: return 'Mile (1201m-1650m)'
            else: return 'Stayer (>1650m)'
        except:
            return 'Mile (1201m-1650m)' # Baseline fallback

    import re
    df['regime'] = df['race_dist'].apply(map_distance_regime)

    exclude_metadata = [
        'date', 'race_id', 'horse_id', 'finish_position', 'public_odds', 'dividend', 'Race_No', 'plc', 
        'is_win', 'is_place', 'is_quinella_hit', 'source_file', 'race_name', 'going', 'race_type', 
        'race_dist', 'horse_name', 'jockey_name', 'trainer_name', 'running_pos', 'finish_time', 'lbw', 
        'parsed_lbw', 'raw_ESI', 'raw_CSI', 'regime'
    ]
    features = [col for col in df.columns if col not in exclude_metadata]
    features = df[features].select_dtypes(include=[np.number, bool]).columns.tolist()
    
    train_df = df[df['date'] < '2022-01-01'].copy()
    calib_df = df[(df['date'] >= '2022-01-01') & (df['date'] < '2023-01-01')].copy()
    test_df = df[df['date'] >= '2023-01-01'].copy()

    # Train Global Engine
    ranker = train_ranker(train_df, features)
    
    calib_df['model_score'] = ranker.predict(calib_df[features])
    test_df['model_score'] = ranker.predict(test_df[features])
    test_df['model_rank'] = test_df.groupby('race_id')['model_score'].rank(ascending=False, method='dense')
    
    calibrator = PlaceProbabilityCalibrator()
    calibrator.fit(calib_df, score_col='model_score', target_col='is_place')
    test_df = calibrator.transform(test_df, score_col='model_score')

    # Audit via Blocks
    regimes = test_df['regime'].unique()
    regime_results = []

    logging.info("\nAuditing Platt Calibration Accuracy across Distance Blocks...")
    
    for regime in regimes:
        block = test_df[test_df['regime'] == regime].copy()
        if len(block) == 0: continue
            
        brier = brier_score_loss(block['is_place'], block['calibrated_place_prob'])
        
        # Calculate specific Tierce hit rate within this block
        races_hit = 0
        races_bet = 0
        races = block.groupby('race_id')
        
        for race_id, race_data in races:
            anchors = race_data[(race_data['model_rank'] == 1.0) & (race_data['public_odds'] <= 5.0)]
            if anchors.empty: continue
            anchor = anchors.iloc[0]
            
            legs = race_data[(race_data['horse_id'] != anchor['horse_id']) & (race_data['public_odds'] >= 7.0)].sort_values(by='calibrated_place_prob', ascending=False).head(4)
            if len(legs) < 4: continue
                
            races_bet += 1
            win = (anchor['finish_position'] == 1.0) and (legs[legs['finish_position'] <= 3.0].shape[0] >= 2)
            if win: races_hit += 1
                
        hit_rate = (races_hit / races_bet * 100) if races_bet > 0 else 0.0
        
        regime_results.append({
            'Regime_Block': regime,
            'Sample_Size': len(block),
            'Brier_Score': brier,
            'Executions': races_bet,
            'Tierce_Hit_Rate_%': hit_rate
        })

    results_df = pd.DataFrame(regime_results)
    print("\n=== REGIME BLOCK STABILITY REPORT ===")
    print(results_df.to_string(index=False, float_format="%.4f"))

if __name__ == "__main__":
    run_regime_audit()