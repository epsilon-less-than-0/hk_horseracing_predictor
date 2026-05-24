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
        logging.info("Fitting Platt Scaling for P(Place)...")
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

def calculate_kelly_fraction(p_win, decimal_odds, kelly_multiplier=0.25):
    b = decimal_odds - 1.0
    q = 1.0 - p_win
    f_star = (b * p_win - q) / b
    if f_star <= 0: return 0.0
    return f_star * kelly_multiplier

def run_production_desk():
    logging.info("=== PHASE 41: PRODUCTION TIERCE DESK (V12 FULL MATRIX) ===")
    
    conn = sqlite3.connect('hk_racing.db')
    df = pd.read_sql("SELECT * FROM ml_features_v12", conn)
    conn.close()

    if 'plc' in df.columns: df.rename(columns={'plc': 'finish_position'}, inplace=True)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['race_id'] = pd.to_numeric(df['race_id'], errors='coerce')
    df['horse_id'] = df['horse_id'].astype(str).str.strip()
    df.dropna(subset=['date', 'race_id', 'horse_id'], inplace=True)

    df['finish_position'] = df['finish_position'].astype(str).str.extract(r'(\d+)').astype(float)
    df['public_odds'] = pd.to_numeric(df['public_odds'], errors='coerce')
    df['is_place'] = (df['finish_position'] <= 3).astype(int)
    df.dropna(subset=['public_odds', 'finish_position'], inplace=True)

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
    calib_df = df[(df['date'] >= '2022-01-01') & (df['date'] < '2023-01-01')].copy()
    test_df = df[df['date'] >= '2023-01-01'].copy()

    ranker = train_ranker(train_df, features)
    calib_df['model_score'] = ranker.predict(calib_df[features])
    test_df['model_score'] = ranker.predict(test_df[features])
    test_df['model_rank'] = test_df.groupby('race_id')['model_score'].rank(ascending=False, method='dense')
    
    calibrator = PlaceProbabilityCalibrator()
    calibrator.fit(calib_df, score_col='model_score', target_col='is_place')
    test_df = calibrator.transform(test_df, score_col='model_score')

    # --- THE PRODUCTION LIQUIDITY DESK ---
    logging.info(f"\nExecuting Constrained Kelly Tierce Simulator on {len(test_df['race_id'].unique())} races...")
    
    # Capital & Market Constraints
    BANKROLL = 100_000.0
    MIN_BASE_BET = 10.0
    N_LEGS = 4
    TICKET_COMBINATIONS = N_LEGS * (N_LEGS - 1) # 12
    
    # Financial Projections (Based on Phase 40 Telemetry)
    EMPIRICAL_WIN_PROB = 0.0768 
    EXPECTED_TIERCE_DIVIDEND = 2500.0 # Expected payout per $10 base ticket
    EXPECTED_DECIMAL_ODDS = EXPECTED_TIERCE_DIVIDEND / 10.0 # 250.0
    KELLY_MULT = 0.25
    
    # Liquidity Caps
    ESTIMATED_TIERCE_POOL = 15_000_000.0 # HKD
    MAX_POOL_PERCENTAGE = 0.02
    LIQUIDITY_CAP = ESTIMATED_TIERCE_POOL * MAX_POOL_PERCENTAGE
    
    base_kelly_fraction = calculate_kelly_fraction(EMPIRICAL_WIN_PROB, EXPECTED_DECIMAL_ODDS, KELLY_MULT)
    logging.info(f"Target Quarter-Kelly Risk: {base_kelly_fraction*100:.2f}% of Bankroll")
    logging.info(f"Hard Liquidity Ceiling: ${LIQUIDITY_CAP:,.2f} HKD per race")
    
    total_action = 0
    tierces_hit = 0
    races_bet = 0
    max_drawdown = 0
    peak_bankroll = BANKROLL
    capped_executions = 0
    
    races = test_df.groupby(['date', 'race_id'])
    
    for (r_date, r_id), race_data in races:
        if BANKROLL <= (MIN_BASE_BET * TICKET_COMBINATIONS):
            logging.error("BANKRUPTCY TRIGGERED. Ceasing execution.")
            break
            
        anchors = race_data[(race_data['model_rank'] == 1.0) & (race_data['public_odds'] <= 5.0)]
        if anchors.empty: continue
        anchor = anchors.iloc[0]
        
        legs = race_data[
            (race_data['horse_id'] != anchor['horse_id']) & (race_data['public_odds'] >= 7.0)
        ].sort_values(by='calibrated_place_prob', ascending=False).head(N_LEGS)
        
        if len(legs) < N_LEGS: continue
            
        # 1. Calculate Target Wager
        target_risk = BANKROLL * base_kelly_fraction
        
        # 2. Enforce Combinatorial Logic (Must be divisible by 12 combinations and >= minimums)
        base_bet_per_combo = max(MIN_BASE_BET, round((target_risk / TICKET_COMBINATIONS) / 10.0) * 10.0)
        proposed_total_wager = base_bet_per_combo * TICKET_COMBINATIONS
        
        # 3. Enforce Liquidity Cap
        if proposed_total_wager > LIQUIDITY_CAP:
            # Scale down to the cap, preserving combinatorics
            base_bet_per_combo = round((LIQUIDITY_CAP / TICKET_COMBINATIONS) / 10.0) * 10.0
            proposed_total_wager = base_bet_per_combo * TICKET_COMBINATIONS
            capped_executions += 1
            
        BANKROLL -= proposed_total_wager
        total_action += proposed_total_wager
        races_bet += 1
        
        # Resolution
        if (anchor['finish_position'] == 1.0) and (legs[legs['finish_position'] <= 3.0].shape[0] >= 2):
            tierces_hit += 1
            # Payout equals the base bet multiplier * the expected standard dividend
            payout = (base_bet_per_combo / 10.0) * EXPECTED_TIERCE_DIVIDEND
            BANKROLL += payout
            
        # Drawdown Tracking
        if BANKROLL > peak_bankroll: peak_bankroll = BANKROLL
        drawdown = (peak_bankroll - BANKROLL) / peak_bankroll
        if drawdown > max_drawdown: max_drawdown = drawdown

    if races_bet > 0:
        roi = ((BANKROLL - 100_000.0) / 100_000.0) * 100
        hit_rate = (tierces_hit / races_bet) * 100
        
        logging.info("\n=== THE PRODUCTION TIERCE LEDGER ===")
        logging.info(f"Starting Principal: $100,000.00")
        logging.info(f"Ending Bankroll: ${BANKROLL:,.2f}")
        logging.info(f"Net ROI: {roi:+.2f}%")
        logging.info("-" * 40)
        logging.info(f"Races Bet: {races_bet:,} | Tierces Hit: {tierces_hit:,} | Strike Rate: {hit_rate:.2f}%")
        logging.info(f"Total Capital Deployed: ${total_action:,.2f}")
        logging.info(f"Max Drawdown: {max_drawdown*100:.2f}%")
        logging.info(f"Executions hitting Liquidity Cap: {capped_executions:,}")
        logging.info("-" * 40)

if __name__ == "__main__":
    run_production_desk()