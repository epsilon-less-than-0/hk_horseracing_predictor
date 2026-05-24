import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging
import re
from sklearn.linear_model import LogisticRegression

# Production Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

def map_distance_regime(dist_str):
    try:
        d = int(re.sub(r'\D', '', str(dist_str)))
        if d <= 1200: return 'Sprint'
        elif d <= 1650: return 'Mile'
        else: return 'Stayer'
    except:
        return 'Mile'

def compute_hardened_harville_dividend(anchor_odds, legs_odds_list, all_race_odds, rake=0.175):
    """
    Phase 47.1: Mathematically Pure Harville Synthesis.
    Normalizes the public pool Overround to guarantee strictly positive denominators,
    eliminating infinity anomalies without arbitrary conditional bounds.
    """
    # 1. Calculate the Public Overround
    inv_odds = [1.0 / o for o in all_race_odds if o > 0.0]
    overround = sum(inv_odds)
    
    # 2. Extract normalized true public probabilities (Rake isolated)
    p_anchor = (1.0 / anchor_odds) / overround
    
    # Absolute bounds protection for extreme edge cases
    p_anchor = min(p_anchor, 0.95) 
    
    synthetic_payouts = []
    
    for i in range(len(legs_odds_list)):
        for j in range(len(legs_odds_list)):
            if i == j: continue
            
            p_leg2 = (1.0 / legs_odds_list[i]) / overround
            p_leg3 = (1.0 / legs_odds_list[j]) / overround
            
            # Because probabilities sum to 1.0, denominators are mathematically > 0
            denom2 = max(1.0 - p_anchor, 0.001)
            denom3 = max(1.0 - p_anchor - p_leg2, 0.001)
            
            # Harville formulation
            p_joint = p_anchor * (p_leg2 / denom2) * (p_leg3 / denom3)
            p_joint = max(p_joint, 0.00001) # Final zero-division safeguard
            
            # Payout equals inverse joint probability * the track return rate
            dividend = (1.0 / p_joint) * (1.0 - rake)
            synthetic_payouts.append(dividend)
            
    return float(np.mean(synthetic_payouts))

def calculate_dynamic_kelly(p_win, decimal_odds, multiplier):
    if decimal_odds <= 1.0: return 0.0
    b = decimal_odds - 1.0
    q = 1.0 - p_win
    f_star = (b * p_win - q) / b
    return max(0.0, f_star * multiplier)

def main():
    logging.info("=== INITIALIZING PHASE 47: HARDENED ASYMMETRIC FUND DESK ===")
    
    # 1. Database Extraction
    conn = sqlite3.connect('hk_racing.db')
    df = pd.read_sql("SELECT * FROM ml_features_v12", conn)
    conn.close()

    df.rename(columns={'plc': 'finish_position'}, inplace=True)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['race_id'] = pd.to_numeric(df['race_id'], errors='coerce')
    df['horse_id'] = df['horse_id'].astype(str).str.strip()
    df.dropna(subset=['date', 'race_id', 'horse_id', 'race_dist'], inplace=True)

    df['finish_position'] = df['finish_position'].astype(str).str.extract(r'(\d+)').astype(float)
    df['public_odds'] = pd.to_numeric(df['public_odds'], errors='coerce')
    df['is_place'] = (df['finish_position'] <= 3).astype(int)
    df.dropna(subset=['public_odds', 'finish_position'], inplace=True)

    df['regime'] = df['race_dist'].apply(map_distance_regime)

    # 2. Strict Metadata Quarantine
    exclude_metadata = [
        'date', 'race_id', 'horse_id', 'finish_position', 'public_odds', 'dividend', 'Race_No', 'plc', 
        'is_win', 'is_place', 'is_quinella_hit', 'source_file', 'race_name', 'going', 'race_type', 
        'race_dist', 'horse_name', 'jockey_name', 'trainer_name', 'running_pos', 'finish_time', 'lbw', 
        'parsed_lbw', 'raw_ESI', 'raw_CSI', 'regime'
    ]
    features = [col for col in df.columns if col not in exclude_metadata]
    features = df[features].select_dtypes(include=[np.number, bool]).columns.tolist()
    
    # 3. Temporal Splitting
    train_df = df[df['date'] < '2022-01-01'].copy()
    calib_df = df[(df['date'] >= '2022-01-01') & (df['date'] < '2023-01-01')].copy()
    test_df = df[df['date'] >= '2023-01-01'].copy()

    # 4. Model Training & Calibration
    ranker = train_ranker(train_df, features)
    calib_df['model_score'] = ranker.predict(calib_df[features])
    test_df['model_score'] = ranker.predict(test_df[features])
    test_df['model_rank'] = test_df.groupby('race_id')['model_score'].rank(ascending=False, method='dense')
    
    calibrator = PlaceProbabilityCalibrator()
    calibrator.fit(calib_df, score_col='model_score', target_col='is_place')
    test_df = calibrator.transform(test_df, score_col='model_score')

    # 5. Portfolio Sizing & Filters
    BANKROLL = 100000.0
    MIN_BASE_BET = 10.0
    TICKET_COMBINATIONS = 12
    MIN_TICKET_COST = MIN_BASE_BET * TICKET_COMBINATIONS # $120 HKD
    KELLY_MULT = 0.10 # Hard 1/10th Deci-Kelly
    EV_THRESHOLD = 1.15 # Minimum 15% Edge overlay required to trigger execution
    
    REGIME_HIT_RATES = {
        'Mile': 0.077748,
        'Sprint': 0.055276,
        'Stayer': 0.070000
    }
    
    logging.info("Executing Hardened Portfolio Sweep with Pure Mathematical Harville Restructuring...")
    total_action = 0.0
    tierces_hit = 0
    races_bet = 0
    max_drawdown = 0.0
    peak_bankroll = BANKROLL
    
    races = test_df.sort_values(by=['date', 'race_id']).groupby(['date', 'race_id'], sort=False)
    
    for (r_date, r_id), race_data in races:
        if BANKROLL <= MIN_TICKET_COST:
            logging.error("CRITICAL ERROR: Portfolio Capital Extinction.")
            break
            
        current_regime = race_data['regime'].iloc[0]
        p_engine_regime = REGIME_HIT_RATES.get(current_regime, 0.065)
        
        # Isolate Anchor
        anchors = race_data[(race_data['model_rank'] == 1.0) & (race_data['public_odds'] <= 5.0)]
        if anchors.empty: continue
        anchor = anchors.iloc[0]
        
        # Isolate Legs
        legs = race_data[
            (race_data['horse_id'] != anchor['horse_id']) & (race_data['public_odds'] >= 7.0)
        ].sort_values(by='calibrated_place_prob', ascending=False).head(4)
        
        if len(legs) < 4: continue
            
        # Extract entire race pool context to feed the restructuring algorithm
        all_race_odds = race_data['public_odds'].dropna().tolist()
        if len(all_race_odds) < 5: continue
            
        # Compute Mathematically Sound Synthetic Payout Ratio
        legs_odds = legs['public_odds'].tolist()
        synthetic_payout_ratio = compute_hardened_harville_dividend(anchor['public_odds'], legs_odds, all_race_odds)
        
        decimal_odds_input = synthetic_payout_ratio / TICKET_COMBINATIONS
        
        # --- PHASE 47 GATEKEEPER: THE ASYMMETRIC ALPHA SWEEP ---
        expected_value = p_engine_regime * decimal_odds_input
        if expected_value < EV_THRESHOLD:
            continue # Edge criteria not met. Pass capital to the next card.
            
        # Dynamic Kelly execution on validated alpha signals
        regime_kelly_fraction = calculate_dynamic_kelly(p_engine_regime, decimal_odds_input, KELLY_MULT)
        
        target_risk = BANKROLL * regime_kelly_fraction
        base_bet_per_combo = round((target_risk / TICKET_COMBINATIONS) / 10.0) * 10.0
        proposed_total_wager = base_bet_per_combo * TICKET_COMBINATIONS
        
        if proposed_total_wager < MIN_TICKET_COST:
            continue
            
        # Commit Capital
        BANKROLL -= proposed_total_wager
        total_action += proposed_total_wager
        races_bet += 1
        
        # Settle
        anchor_win = anchor['finish_position'] == 1.0
        legs_in_top_3 = legs[legs['finish_position'] <= 3.0].shape[0]
        
        if anchor_win and (legs_in_top_3 >= 2):
            tierces_hit += 1
            payout = (proposed_total_wager / TICKET_COMBINATIONS) * (synthetic_payout_ratio / 10.0) * 10.0
            BANKROLL += payout
            
        if BANKROLL > peak_bankroll: peak_bankroll = BANKROLL
        drawdown = (peak_bankroll - BANKROLL) / peak_bankroll
        if drawdown > max_drawdown: max_drawdown = drawdown

    roi = ((BANKROLL - 100000.0) / 100000.0) * 100
    hit_rate = (tierces_hit / races_bet * 100) if races_bet > 0 else 0.0
    
    print("\n" + "="*50)
    print("=== FINAL HARDENED PRODUCTION TIERCE LEDGER ===")
    print("="*50)
    print(f"Starting Seed Capital:    $100,000.00 HKD")
    print(f"Ending Portfolio Value:   ${BANKROLL:,.2f} HKD")
    print(f"Net Compounded PnL:       {roi:+.2f}%")
    print(f"Total Capital Deployed:   ${total_action:,.2f} HKD")
    print(f"Systemic Max Drawdown:    {max_drawdown*100:.2f}%")
    print(f"Validated Executions:     {races_bet} Races")
    print(f"Successful Tierce Spikes: {tierces_hit} Hits")
    print(f"Engine Strike Rate:       {hit_rate:.2f}%")
    print("="*50)

if __name__ == "__main__":
    main()