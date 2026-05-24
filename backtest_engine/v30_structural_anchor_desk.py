import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging
import re
import itertools
import os
from sklearn.linear_model import LogisticRegression

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
    logging.info("=== INITIALIZING PHASE 53: STRUCTURAL ANCHOR DESK ===")
    
    # Robust Path Routing for the new directory structure
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(base_dir, '..', 'data', 'hk_racing.db')
    
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("SELECT * FROM ml_features_v12", conn)
    conn.close()

    df.rename(columns={'plc': 'finish_position'}, inplace=True)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['race_id'] = pd.to_numeric(df['race_id'], errors='coerce')
    df['horse_id'] = df['horse_id'].astype(str).str.strip()
    df.dropna(subset=['date', 'race_id', 'horse_id'], inplace=True)

    df['finish_position'] = df['finish_position'].astype(str).str.extract(r'(\d+)').astype(float)
    df['public_odds'] = pd.to_numeric(df['public_odds'], errors='coerce')
    df['is_win'] = (df['finish_position'] == 1.0).astype(int)
    df['is_place'] = (df['finish_position'] <= 3.0).astype(int)
    df.dropna(subset=['public_odds', 'finish_position'], inplace=True)

    exclude_metadata = [
        'date', 'race_id', 'horse_id', 'finish_position', 'public_odds', 'dividend', 'Race_No', 'plc', 
        'is_win', 'is_place', 'is_quinella_hit', 'source_file', 'race_name', 'going', 'race_type', 
        'race_dist', 'horse_name', 'jockey_name', 'trainer_name', 'running_pos', 'finish_time', 'lbw', 
        'parsed_lbw', 'raw_ESI', 'raw_CSI'
    ]
    features = [col for col in df.columns if col not in exclude_metadata]
    features = df[features].select_dtypes(include=[np.number, bool]).columns.tolist()
    
    train_df = df[df['date'] < '2022-01-01'].copy()
    calib_df = df[(df['date'] >= '2022-01-01') & (df['date'] < '2023-01-01')].copy()
    test_df = df[df['date'] >= '2023-01-01'].copy()

    ranker = train_ranker(train_df, features)
    calib_df['model_score'] = ranker.predict(calib_df[features])
    test_df['model_score'] = ranker.predict(test_df[features])
    test_df['model_rank'] = test_df.groupby('race_id')['model_score'].rank(ascending=False, method='dense')
    
    logging.info("Fitting Bimodal Logistic Calibrators (Win & Place)...")
    win_calibrator = ProbabilityCalibrator()
    win_calibrator.fit(calib_df, score_col='model_score', target_col='is_win')
    test_df = win_calibrator.transform(test_df, score_col='model_score', output_col='calibrated_win_prob')
    
    place_calibrator = ProbabilityCalibrator()
    place_calibrator.fit(calib_df, score_col='model_score', target_col='is_place')
    test_df = place_calibrator.transform(test_df, score_col='model_score', output_col='calibrated_place_prob')

    BANKROLL = 100000.0
    MIN_BASE_BET = 10.0
    TICKET_COMBINATIONS = 6 
    MIN_TICKET_COST = MIN_BASE_BET * TICKET_COMBINATIONS 
    KELLY_MULT = 0.10 # 1/10th Deci-Kelly Base
    EV_THRESHOLD = 1.05 
    RAKE = 0.23 
    
    logging.info("Executing Structural Anchor Sweep (1 Banker > 4 Legs)...")
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
        
        race_data['inv_odds'] = 1.0 / race_data['public_odds']
        race_data['prob_pub'] = race_data['inv_odds'] / race_data['inv_odds'].sum()
        race_data['prob_eng_win'] = race_data['calibrated_win_prob'] / race_data['calibrated_win_prob'].sum()
        
        pub_dict = dict(zip(race_data['horse_id'], race_data['prob_pub']))
        eng_win_dict = dict(zip(race_data['horse_id'], race_data['prob_eng_win']))
        
        anchors = race_data[(race_data['model_rank'] == 1.0) & (race_data['public_odds'] <= 5.0)]
        if anchors.empty: continue
        anchor = anchors.iloc[0]
        
        legs = race_data[
            (race_data['horse_id'] != anchor['horse_id']) & (race_data['public_odds'] >= 7.0)
        ].sort_values(by='calibrated_place_prob', ascending=False).head(4)
        if len(legs) < 4: continue
            
        legs_ids = legs['horse_id'].tolist()
        
        aggregate_p_eng = 0.0
        aggregate_synthetic_payouts = []
        
        for leg2_id, leg3_id in itertools.combinations(legs_ids, 2):
            combo = [anchor['horse_id'], leg2_id, leg3_id]
            
            p_joint_pub = compute_unordered_harville_prob(pub_dict, combo)
            p_joint_eng = compute_unordered_harville_prob(eng_win_dict, combo)
            
            combo_odds = (1.0 / p_joint_pub) * (1.0 - RAKE)
            
            aggregate_p_eng += p_joint_eng
            aggregate_synthetic_payouts.append(combo_odds)
            
        avg_synthetic_odds = np.mean(aggregate_synthetic_payouts) / TICKET_COMBINATIONS
        
        block_ev = aggregate_p_eng * avg_synthetic_odds
        if block_ev >= EV_THRESHOLD:
            
            fractional_kelly = calculate_dynamic_kelly(aggregate_p_eng, avg_synthetic_odds, KELLY_MULT)
            target_risk = BANKROLL * fractional_kelly
            base_bet_per_combo = round((target_risk / TICKET_COMBINATIONS) / 10.0) * 10.0
            proposed_total_wager = base_bet_per_combo * TICKET_COMBINATIONS
            
            if proposed_total_wager >= MIN_TICKET_COST:
                BANKROLL -= proposed_total_wager
                total_action += proposed_total_wager
                races_bet += 1
                
                anchor_placed = anchor['finish_position'] <= 3.0
                legs_in_top_3 = legs[legs['finish_position'] <= 3.0].shape[0]
                
                if anchor_placed and (legs_in_top_3 >= 2):
                    trios_hit += 1
                    BANKROLL += (proposed_total_wager / TICKET_COMBINATIONS) * (np.mean(aggregate_synthetic_payouts))
                    
                if BANKROLL > peak_bankroll: peak_bankroll = BANKROLL
                drawdown = (peak_bankroll - BANKROLL) / peak_bankroll
                if drawdown > max_drawdown: max_drawdown = drawdown

    roi = ((BANKROLL - 100000.0) / 100000.0) * 100
    hit_rate = (trios_hit / races_bet * 100) if races_bet > 0 else 0.0
    
    print("\n" + "="*50)
    print("=== FINAL V30 STRUCTURAL ANCHOR LEDGER ===")
    print("="*50)
    print(f"Starting Seed Capital:    $100,000.00 HKD")
    print(f"Ending Portfolio Value:   ${BANKROLL:,.2f} HKD")
    print(f"Net Compounded PnL:       {roi:+.2f}%")
    print(f"Total Capital Deployed:   ${total_action:,.2f} HKD")
    print(f"Systemic Max Drawdown:    {max_drawdown*100:.2f}%")
    print(f"Targeted Executions:      {races_bet} Races")
    print(f"Successful Trio Spikes:   {trios_hit} Block Hits")
    print(f"Precision Strike Rate:    {hit_rate:.2f}%")
    print("="*50)

if __name__ == "__main__":
    main()