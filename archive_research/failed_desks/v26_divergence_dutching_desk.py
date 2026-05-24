import sqlite3
import pandas as pd
import numpy as np
import xgboost as xgb
import logging
import re
import itertools
from scipy.special import softmax

# Production Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    Computes the unordered Top-3 joint probability for a specific 3-horse combination 
    using the Harville formula across all 6 possible finishing permutations.
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
    logging.info("=== INITIALIZING PHASE 49: COMBINATORIAL DIVERGENCE DESK ===")
    
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
    test_df = df[df['date'] >= '2023-01-01'].copy()

    ranker = train_ranker(train_df, features)
    test_df['model_score'] = ranker.predict(test_df[features])

    # --- THE DIVERGENCE PORTFOLIO ARCHITECTURE ---
    BANKROLL = 100000.0
    MIN_TICKET_COST = 10.0 # HKJC Minimum per combination
    KELLY_MULT = 0.10 # 1/10th Deci-Kelly
    EV_THRESHOLD = 1.05 # Minimum 5% Orthogonal Edge
    RAKE = 0.23 # Trio Takeout
    
    logging.info("Executing Dynamic Fractional Kelly Sweep...")
    total_action = 0.0
    trios_hit = 0
    races_bet = 0
    max_drawdown = 0.0
    peak_bankroll = BANKROLL
    
    races = test_df.sort_values(by=['date', 'race_id']).groupby(['date', 'race_id'], sort=False)
    
    for (r_date, r_id), race_data in races:
        if BANKROLL <= MIN_TICKET_COST:
            logging.error("CRITICAL ERROR: Portfolio Capital Extinction.")
            break
            
        if len(race_data) < 7: continue # Skip field sizes too small for healthy combinatorial variance
        
        # 1. Map Public Probability Universe (Overround Normalized)
        race_data = race_data.copy()
        race_data['inv_odds'] = 1.0 / race_data['public_odds']
        overround = race_data['inv_odds'].sum()
        race_data['prob_pub'] = race_data['inv_odds'] / overround
        
        # 2. Map Engine Probability Universe (Softmax on raw Top-N Logits)
        # We isolate the Top 7 ranked horses to minimize tail-noise computing
        top_7 = race_data.nlargest(7, 'model_score').copy()
        top_7['prob_eng'] = softmax(top_7['model_score'].values)
        
        pub_dict = dict(zip(top_7['horse_id'], top_7['prob_pub']))
        eng_dict = dict(zip(top_7['horse_id'], top_7['prob_eng']))
        horse_ids = top_7['horse_id'].tolist()
        
        race_action = 0.0
        race_payout = 0.0
        combinations_bet = 0
        
        # 3. Combinatorial Dutching Sweep: C(7,3) = 35 Possible Targets
        for combo in itertools.combinations(horse_ids, 3):
            p_joint_pub = compute_unordered_harville_prob(pub_dict, combo)
            p_joint_eng = compute_unordered_harville_prob(eng_dict, combo)
            
            # Synthetic Payout for this specific ticket
            combo_odds = (1.0 / p_joint_pub) * (1.0 - RAKE)
            
            # Asymmetric Gatekeeper
            combo_ev = p_joint_eng * combo_odds
            if combo_ev < EV_THRESHOLD:
                continue
                
            # Exact fractional target for this specific combination's localized edge
            fractional_kelly = calculate_dynamic_kelly(p_joint_eng, combo_odds, KELLY_MULT)
            
            ticket_wager = round((BANKROLL * fractional_kelly) / 10.0) * 10.0
            if ticket_wager < MIN_TICKET_COST:
                continue
                
            race_action += ticket_wager
            combinations_bet += 1
            
            # Resolution Tracking for this specific ticket
            combo_df = top_7[top_7['horse_id'].isin(combo)]
            if combo_df[combo_df['finish_position'] <= 3.0].shape[0] == 3:
                trios_hit += 1
                race_payout += (ticket_wager / 10.0) * (combo_odds * 10.0)
                
        # Portfolio Settle
        if combinations_bet > 0:
            races_bet += 1
            BANKROLL -= race_action
            total_action += race_action
            BANKROLL += race_payout
            
            if BANKROLL > peak_bankroll: peak_bankroll = BANKROLL
            drawdown = (peak_bankroll - BANKROLL) / peak_bankroll
            if drawdown > max_drawdown: max_drawdown = drawdown

    roi = ((BANKROLL - 100000.0) / 100000.0) * 100
    hit_rate = (trios_hit / races_bet * 100) if races_bet > 0 else 0.0
    
    print("\n" + "="*50)
    print("=== FINAL V26 DIVERGENCE DUTCHING LEDGER ===")
    print("="*50)
    print(f"Starting Seed Capital:    $100,000.00 HKD")
    print(f"Ending Portfolio Value:   ${BANKROLL:,.2f} HKD")
    print(f"Net Compounded PnL:       {roi:+.2f}%")
    print(f"Total Capital Deployed:   ${total_action:,.2f} HKD")
    print(f"Systemic Max Drawdown:    {max_drawdown*100:.2f}%")
    print(f"Races with Action Taken:  {races_bet} Races")
    print(f"Successful Trio Spikes:   {trios_hit} Exact Combinations Hit")
    print(f"Actionable Strike Rate:   {hit_rate:.2f}%")
    print("="*50)

if __name__ == "__main__":
    main()