import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging
from sklearn.linear_model import LogisticRegression

logging.basicConfig(level=logging.INFO, format='%(message)s')

class PlaceProbabilityCalibrator:
    def __init__(self):
        self.calibrator = LogisticRegression(solver='lbfgs', max_iter=500)
    def fit(self, validation_df: pd.DataFrame, score_col: str = 'model_score', target_col: str = 'is_place'):
        X_val = validation_df[score_col].values.reshape(-1, 1)
        self.calibrator.fit(X_val, validation_df[target_col].values)
    def transform(self, test_df: pd.DataFrame, score_col: str = 'model_score') -> pd.DataFrame:
        test_df['calibrated_place_prob'] = self.calibrator.predict_proba(test_df[score_col].values.reshape(-1, 1))[:, 1]
        return test_df

def train_ranker(train_df, features):
    logging.info(f"Training XGBRanker Oracle on {len(train_df['race_id'].unique())} historical races...")
    train_df = train_df.sort_values('race_id')
    ranker = xgb.XGBRanker(tree_method='hist', objective='rank:pairwise', learning_rate=0.05, max_depth=4, colsample_bytree=0.5, n_estimators=150, random_state=42)
    ranker.fit(train_df[features], 20 - train_df['finish_position'].fillna(20), group=train_df.groupby('race_id').size().values)
    return ranker

def calculate_kelly_fraction(p_win, decimal_odds, kelly_multiplier):
    b = decimal_odds - 1.0
    f_star = (b * p_win - (1.0 - p_win)) / b
    return max(0.0, f_star * kelly_multiplier)

def run_monte_carlo_desk():
    logging.info("=== PHASE 42: MONTE CARLO RISK DESK (V12 FULL MATRIX) ===")
    
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
    
    calibrator = PlaceProbabilityCalibrator()
    calibrator.fit(calib_df, score_col='model_score', target_col='is_place')
    test_df = calibrator.transform(test_df, score_col='model_score')

    # 1. Pre-calculate all valid Tierce opportunities
    logging.info("Extracting Valid Tierce Opportunities...")
    valid_executions = []
    
    races = test_df.groupby('race_id')
    for race_id, race_data in races:
        anchors = race_data[(race_data['model_rank'] == 1.0) & (race_data['public_odds'] <= 5.0)]
        if anchors.empty: continue
        anchor = anchors.iloc[0]
        
        legs = race_data[(race_data['horse_id'] != anchor['horse_id']) & (race_data['public_odds'] >= 7.0)].sort_values(by='calibrated_place_prob', ascending=False).head(4)
        if len(legs) < 4: continue
            
        win = (anchor['finish_position'] == 1.0) and (legs[legs['finish_position'] <= 3.0].shape[0] >= 2)
        valid_executions.append({'race_id': race_id, 'is_win': win})

    exec_df = pd.DataFrame(valid_executions)
    N_RACES = len(exec_df)
    
    # 2. Monte Carlo Parameters
    SIMULATIONS = 1000
    START_BANKROLL = 100_000.0
    MIN_BASE_BET = 10.0
    TICKET_COMBINATIONS = 12
    MIN_TICKET_COST = MIN_BASE_BET * TICKET_COMBINATIONS
    EXPECTED_TIERCE_DIVIDEND = 2500.0
    EXPECTED_DECIMAL_ODDS = 250.0 
    EMPIRICAL_WIN_PROB = exec_df['is_win'].mean()
    
    # We test 1/10th Kelly to survive massive variance
    KELLY_MULT = 0.10 
    base_kelly_fraction = calculate_kelly_fraction(EMPIRICAL_WIN_PROB, EXPECTED_DECIMAL_ODDS, KELLY_MULT)
    
    logging.info(f"\nTarget 1/10th Kelly Risk: {base_kelly_fraction*100:.3f}% of Bankroll")
    logging.info(f"Running {SIMULATIONS} Monte Carlo random walk simulations...")

    # 3. Execution Engine
    final_bankrolls = []
    ruined_count = 0
    total_bets_taken = []
    
    for i in range(SIMULATIONS):
        bankroll = START_BANKROLL
        shuffled_races = exec_df.sample(frac=1).reset_index(drop=True)
        bets_taken = 0
        
        for _, race in shuffled_races.iterrows():
            target_risk = bankroll * base_kelly_fraction
            base_bet_per_combo = round((target_risk / TICKET_COMBINATIONS) / 10.0) * 10.0
            proposed_total_wager = base_bet_per_combo * TICKET_COMBINATIONS
            
            # THE MINIMUM BET SURVIVAL RULE
            if proposed_total_wager < MIN_TICKET_COST:
                continue # Bankroll too low to safely bet 12 combos. Pass the race.
                
            bankroll -= proposed_total_wager
            bets_taken += 1
            
            if race['is_win']:
                bankroll += (base_bet_per_combo / 10.0) * EXPECTED_TIERCE_DIVIDEND
                
        final_bankrolls.append(bankroll)
        total_bets_taken.append(bets_taken)
        if bankroll < MIN_TICKET_COST:
            ruined_count += 1

    # 4. Monte Carlo Telemetry Analysis
    median_ending_bankroll = np.median(final_bankrolls)
    mean_ending_bankroll = np.mean(final_bankrolls)
    prob_of_ruin = (ruined_count / SIMULATIONS) * 100
    avg_bets_taken = np.mean(total_bets_taken)
    
    logging.info("\n=== MONTE CARLO RISK LEDGER (1,000 ITERATIONS) ===")
    logging.info(f"Starting Principal: ${START_BANKROLL:,.2f}")
    logging.info(f"Median Ending Bankroll: ${median_ending_bankroll:,.2f} (Expected path)")
    logging.info(f"Mean Ending Bankroll: ${mean_ending_bankroll:,.2f} (Pulled by positive outliers)")
    logging.info(f"Probability of Ruin (Bankrupt): {prob_of_ruin:.2f}%")
    logging.info(f"Average Executions (Races actually bet): {avg_bets_taken:.0f} / {N_RACES}")
    logging.info("-" * 50)
    logging.info("NOTE: By passing races when Kelly sizing falls below the HKJC minimum ($120),")
    logging.info("we eliminate forced over-betting and mathematical certainty of ruin.")

if __name__ == "__main__":
    run_monte_carlo_desk()