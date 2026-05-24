import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging
import re
from sklearn.linear_model import LogisticRegression

# Setup production logging
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

def calculate_dynamic_kelly(p_win, decimal_odds, multiplier):
    b = decimal_odds - 1.0
    q = 1.0 - p_win
    f_star = (b * p_win - q) / b
    return max(0.0, f_star * multiplier)

def main():
    logging.info("=== INITIALIZING PHASE 44: REGIME-WEIGHTED PRODUCTION DESK ===")
    
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

    # 5. Production Simulation Parameters
    BANKROLL = 100000.0
    MIN_BASE_BET = 10.0
    TICKET_COMBINATIONS = 12
    MIN_TICKET_COST = MIN_BASE_BET * TICKET_COMBINATIONS # $120 HKD
    EXPECTED_TIERCE_DIVIDEND = 2500.0
    EXPECTED_DECIMAL_ODDS = 250.0
    KELLY_MULT = 0.10 # Hard 1/10th Kelly parameter
    
    # Empirical Multipliers mapped from Stress Desk Telemetry
    REGIME_HIT_RATES = {
        'Mile': 0.077748,
        'Sprint': 0.055276,
        'Stayer': 0.070000  # Conservative estimate for staying races
    }
    
    logging.info("Executing Live Simulated Portfolio Compounding...")
    total_action = 0.0
    tierces_hit = 0
    races_bet = 0
    max_drawdown = 0.0
    peak_bankroll = BANKROLL
    
    # Chronological simulated execution loop
    races = test_df.sort_values(by=['date', 'race_id']).groupby(['date', 'race_id'], sort=False)
    
    for (r_date, r_id), race_data in races:
        if BANKROLL <= MIN_TICKET_COST:
            logging.error("EMERGENCY STOP: Portfolio Liquidity Exhausted.")
            break
            
        current_regime = race_data['regime'].iloc[0]
        p_regime = REGIME_HIT_RATES.get(current_regime, 0.065)
        
        # Calculate specialized Kelly fraction for the detected physical block
        regime_kelly_fraction = calculate_dynamic_kelly(p_regime, EXPECTED_DECIMAL_ODDS, KELLY_MULT)
        
        # Find Anchor (Favorite with verified model ranking)
        anchors = race_data[(race_data['model_rank'] == 1.0) & (race_data['public_odds'] <= 5.0)]
        if anchors.empty: continue
        anchor = anchors.iloc[0]
        
        # Find Volatility Legs (Ignore public consensus, optimize for Glicko-2 RD and PageRank)
        legs = race_data[
            (race_data['horse_id'] != anchor['horse_id']) & (race_data['public_odds'] >= 7.0)
        ].sort_values(by='calibrated_place_prob', ascending=False).head(4)
        
        if len(legs) < 4: continue
            
        # Target sizing driven dynamically by current regime block accuracy
        target_risk = BANKROLL * regime_kelly_fraction
        base_bet_per_combo = round((target_risk / TICKET_COMBINATIONS) / 10.0) * 10.0
        proposed_total_wager = base_bet_per_combo * TICKET_COMBINATIONS
        
        # Circuit Breaker: Prevent forced over-betting on tail-risk events
        if proposed_total_wager < MIN_TICKET_COST:
            continue
            
        # Commit Capital
        BANKROLL -= proposed_total_wager
        total_action += proposed_total_wager
        races_bet += 1
        
        # Settle Contract
        anchor_win = anchor['finish_position'] == 1.0
        legs_in_top_3 = legs[legs['finish_position'] <= 3.0].shape[0]
        
        if anchor_win and (legs_in_top_3 >= 2):
            tierces_hit += 1
            payout = (base_bet_per_combo / 10.0) * EXPECTED_TIERCE_DIVIDEND
            BANKROLL += payout
            
        # Performance Tracking Metrics
        if BANKROLL > peak_bankroll: peak_bankroll = BANKROLL
        drawdown = (peak_bankroll - BANKROLL) / peak_bankroll
        if drawdown > max_drawdown: max_drawdown = drawdown

    # 6. Final Production Manifest
    roi = ((BANKROLL - 100000.0) / 100000.0) * 100
    hit_rate = (tierces_hit / races_bet * 100) if races_bet > 0 else 0.0
    
    print("\n" + "="*50)
    print("=== FINAL PRODUCTION EXECUTION LEDGER ===")
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