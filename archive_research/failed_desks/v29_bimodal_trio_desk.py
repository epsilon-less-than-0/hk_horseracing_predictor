import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging
import re
import itertools
from sklearn.linear_model import LogisticRegression

# Production Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ProbabilityCalibrator:
    def __init__(self):
        self.calibrator = LogisticRegression(solver='lbfgs', max_iter=500)
        
    def fit(self, validation_df: pd.DataFrame, score_col: str, target_col: str):
        X_val = validation_df[score_col].values.reshape(-1, 1)
        self.calibrator.fit(X_val, validation_df[target_col].values)
        
    def transform(self, test_df: pd.DataFrame, score_col: str, output_col: str) -> pd.DataFrame:
        X_test = test_df[score_col].values.reshape(-1, 1)
        test_df[output_col] = self.calibrator.predict_proba(X_test)[:, 1]
        return test_df

def train_ranker(train_df, features):
    logging.info(f"Training Master XGBRanker Engine on {len(train_df['race_id'].unique())} races...")
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

def compute_unordered_harville_prob(probs_dict, combo):
    """
    Computes the unordered Top-3 joint probability for a specific 3-horse combination.
    Requires mathematically pure P(Win) maps that sum to 1.0.
    """
    p1, p2, p3 = probs_dict[combo[0]], probs_dict[combo[1]], probs_dict[combo[2]]
    trio_prob = 0.0
    
    for a, b, c in itertools.permutations([p1, p2, p3]):
        denom1 = max(1.0 - a, 0.0001)
        denom2 = max(1.0 - a - b, 0.0001)
        trio_prob += a * (b / denom1) * (c / denom2)
        
    return max(trio_prob, 0.000001)

def calculate_dynamic_kelly(p_win, decimal_odds, multiplier):
    if decimal_odds <= 1.0: return 0.0
    b = decimal_odds - 1.0
    f_star = (b * p_win - (1.0 - p_win)) / b
    return max(0.0, f_star * multiplier)

def main():
    logging.info("=== INITIALIZING PHASE 52: BIMODAL MAX-ALPHA DESK ===")
    
    # 1. Database Extraction
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
    
    # Bimodal Targets
    df['is_win'] = (df['finish_position'] == 1.0).astype(int)
    df['is_place'] = (df['finish_position'] <= 3.0).astype(int)
    df.dropna(subset=['public_odds', 'finish_position'], inplace=True)

    # 2. Strict Metadata Quarantine
    exclude_metadata = [
        'date', 'race_id', 'horse_id', 'finish_position', 'public_odds', 'dividend', 'Race_No', 'plc', 
        'is_win', 'is_place', 'is_quinella_hit', 'source_file', 'race_name', 'going', 'race_type', 
        'race_dist', 'horse_name', 'jockey_name', 'trainer_name', 'running_pos', 'finish_time', 'lbw', 
        'parsed_lbw', 'raw_ESI', 'raw_CSI'
    ]
    features = [col for col in df.columns if col not in exclude_metadata]
    features = df[features].select_dtypes(include=[np.number, bool]).columns.tolist()
    
    # 3. Temporal Splitting
    train_df = df[df['date'] < '2022-01-01'].copy()
    calib_df = df[(df['date'] >= '2022-01-01') & (df['date'] < '2023-01-01')].copy()
    test_df = df[df['date'] >= '2023-01-01'].copy()

    # 4. Engine Training
    ranker = train_ranker(train_df, features)
    calib_df['model_score'] = ranker.predict(calib_df[features])
    test_df['model_score'] = ranker.predict(test_df[features])
    
    # 5. Bimodal Calibration
    logging.info("Fitting Bimodal Logistic Calibrators (Win & Place)...")
    
    win_calibrator = ProbabilityCalibrator()
    win_calibrator.fit(calib_df, score_col='model_score', target_col='is_win')
    test_df = win_calibrator.transform(test_df, score_col='model_score', output_col='calibrated_win_prob')
    
    place_calibrator = ProbabilityCalibrator()
    place_calibrator.fit(calib_df, score_col='model_score', target_col='is_place')
    test_df = place_calibrator.transform(test_df, score_col='model_score', output_col='calibrated_place_prob')

    # --- THE BIMODAL PORTFOLIO ARCHITECTURE ---
    BANKROLL = 100000.0
    MIN_TICKET_COST = 10.0 # Absolute floor for a single Trio combination
    KELLY_MULT = 0.10 # 1/10th Deci-Kelly
    EV_THRESHOLD = 1.05 # Minimum 5% Edge
    MIN_KINETIC_FLOOR = 0.01 # Minimum 1.0% absolute chance of occurring to prevent EV Mirages
    RAKE = 0.23 # Exact HKJC Trio Takeout
    
    logging.info("Executing Bimodal Harville Sweep with Anti-Mirage Gatekeepers...")
    total_action = 0.0
    trios_hit = 0
    races_bet = 0
    max_drawdown = 0.0
    peak_bankroll = BANKROLL
    
    races = test_df.sort_values(by=['date', 'race_id']).groupby(['date', 'race_id'], sort=False)
    
    for (r_date, r_id), race_data in races:
        if BANKROLL <= MIN_TICKET_COST: break
        if len(race_data) < 7: continue 
        
        race_data = race_data.copy()
        
        # A. Normalize Public P(Win)
        race_data['inv_odds'] = 1.0 / race_data['public_odds']
        race_data['prob_pub'] = race_data['inv_odds'] / race_data['inv_odds'].sum()
        
        # B. Normalize Engine P(Win) - Mathematically pure for Harville
        race_data['prob_eng_win'] = race_data['calibrated_win_prob'] / race_data['calibrated_win_prob'].sum()
        
        # C. Target Selection - Use P(Place) to capture Closers and High-Variance threats
        top_7 = race_data.nlargest(7, 'calibrated_place_prob').copy()
        
        pub_dict = dict(zip(top_7['horse_id'], top_7['prob_pub']))
        eng_win_dict = dict(zip(top_7['horse_id'], top_7['prob_eng_win']))
        horse_ids = top_7['horse_id'].tolist()
        
        best_combo = None
        best_kelly = 0.0
        best_odds = 0.0
        
        # D. Combinatorial Alpha Sweep
        for combo in itertools.combinations(horse_ids, 3):
            # Calculate joint probabilities using strictly normalized P(Win) distributions
            p_joint_pub = compute_unordered_harville_prob(pub_dict, combo)
            p_joint_eng = compute_unordered_harville_prob(eng_win_dict, combo)
            
            combo_odds = (1.0 / p_joint_pub) * (1.0 - RAKE)
            combo_ev = p_joint_eng * combo_odds
            
            # THE DUAL GATEKEEPER: Edge must be real (> 1.05) AND Physics must be viable (> 1.0% chance)
            if combo_ev >= EV_THRESHOLD and p_joint_eng >= MIN_KINETIC_FLOOR:
                fractional_kelly = calculate_dynamic_kelly(p_joint_eng, combo_odds, KELLY_MULT)
                
                # MAX-ALPHA ISOLATION
                if fractional_kelly > best_kelly:
                    best_kelly = fractional_kelly
                    best_combo = combo
                    best_odds = combo_odds
                    
        # E. Execution Module
        if best_combo is not None:
            ticket_wager = round((BANKROLL * best_kelly) / 10.0) * 10.0
            
            if ticket_wager >= MIN_TICKET_COST:
                BANKROLL -= ticket_wager
                total_action += ticket_wager
                races_bet += 1
                
                # Settlement Module
                combo_df = top_7[top_7['horse_id'].isin(best_combo)]
                if combo_df[combo_df['finish_position'] <= 3.0].shape[0] == 3:
                    trios_hit += 1
                    payout = (ticket_wager / 10.0) * (best_odds * 10.0)
                    BANKROLL += payout
                    
                # Track Drawdown
                if BANKROLL > peak_bankroll: peak_bankroll = BANKROLL
                drawdown = (peak_bankroll - BANKROLL) / peak_bankroll
                if drawdown > max_drawdown: max_drawdown = drawdown

    roi = ((BANKROLL - 100000.0) / 100000.0) * 100
    hit_rate = (trios_hit / races_bet * 100) if races_bet > 0 else 0.0
    
    print("\n" + "="*50)
    print("=== FINAL V29 BIMODAL TRIO LEDGER ===")
    print("="*50)
    print(f"Starting Seed Capital:    $100,000.00 HKD")
    print(f"Ending Portfolio Value:   ${BANKROLL:,.2f} HKD")
    print(f"Net Compounded PnL:       {roi:+.2f}%")
    print(f"Total Capital Deployed:   ${total_action:,.2f} HKD")
    print(f"Systemic Max Drawdown:    {max_drawdown*100:.2f}%")
    print(f"Targeted Executions:      {races_bet} Races")
    print(f"Successful Trio Spikes:   {trios_hit} Apex Hits")
    print(f"Precision Strike Rate:    {hit_rate:.2f}%")
    print("="*50)

if __name__ == "__main__":
    main()