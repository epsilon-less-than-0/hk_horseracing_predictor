import sqlite3
import pandas as pd
import xgboost as xgb
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def run_divergence_hunter():
    logging.info("=== PHASE 7: THE DIVERGENCE HUNTER (V4.0) ===")
    
    # 1. Load the V2.0 Alpha Model
    try:
        ranker = xgb.XGBRanker()
        ranker.load_model('v2_hkjc_ranker.json')
    except Exception as e:
        logging.error("Could not load V2.0 model.")
        return

    # 2. Extract Data 
    conn = sqlite3.connect('hk_racing.db')
    df_features = pd.read_sql("SELECT * FROM ml_features", conn)
    df_odds = pd.read_sql("SELECT date, race_id, horse_id, public_odds as win_odds FROM race_results", conn)
    conn.close()

    df_features['date'] = pd.to_datetime(df_features['date']).dt.normalize()
    df_odds['date'] = pd.to_datetime(df_odds['date']).dt.normalize()
    df = pd.merge(df_features, df_odds, on=['date', 'race_id', 'horse_id'], how='inner')

    df['plc_num'] = pd.to_numeric(df['plc'], errors='coerce')
    df['win_odds'] = pd.to_numeric(df['win_odds'], errors='coerce')
    
    # Filter for Unseen Holdout Data
    test_df = df[(df['date'].dt.year >= 2025)].dropna(subset=['plc_num', 'win_odds']).copy()
    test_df = test_df.sort_values(by=['date', 'race_id'])
    
    features = [
        'pre_race_elo', 'days_since_last_run', 'weight_delta', 
        'distance_delta', 'career_wins', 'is_turf', 
        'draw', 'jockey_win_pct', 'trainer_win_pct'
    ]
    X_test = test_df[features].astype(float)
    
    # 3. Generate AI Rankings AND Public Rankings
    logging.info(f"Scanning {len(test_df)} runners for Market Divergence...")
    if len(test_df) == 0: return
        
    # AI Rank
    test_df['raw_score'] = ranker.predict(X_test)
    test_df['model_rank'] = test_df.groupby(['date', 'race_id'])['raw_score'].rank(ascending=False, method='min')
    
    # Public Rank (Lowest Odds = Rank 1)
    test_df['public_rank'] = test_df.groupby(['date', 'race_id'])['win_odds'].rank(ascending=True, method='min')
    
    # 4. Isolate Model's #1 Picks
    top_picks = test_df[test_df['model_rank'] == 1.0].copy()
    
    # Group them by where the PUBLIC ranked them
    # e.g., Public Rank 1 = "Consensus Favorite"
    # e.g., Public Rank 4 = "Ignored by the Crowd"
    
    logging.info("\n=== DIVERGENCE ANALYSIS (AI #1 PICKS) ===")
    logging.info(f"{'Public Rank':<15} | {'Bets':<5} | {'Wins':<5} | {'Win %':<6} | {'ROI':<8}")
    logging.info("-" * 55)
    
    total_bets = 0
    total_returned = 0.0
    
    # We will look at the top 6 public ranks to find the sweet spot
    for pub_rank in range(1, 7):
        tier_data = top_picks[top_picks['public_rank'] == pub_rank]
        bets = len(tier_data)
        if bets == 0: continue
        
        wins = len(tier_data[tier_data['plc_num'] == 1.0])
        win_pct = (wins / bets) * 100
        
        wagered = bets * 100.0
        returned = tier_data[tier_data['plc_num'] == 1.0]['win_odds'].sum() * 100.0
        roi = ((returned - wagered) / wagered) * 100
        
        total_bets += bets
        total_returned += returned
        
        # Labeling for clarity
        label = f"Rank {pub_rank}"
        if pub_rank == 1: label += " (Agree)"
        elif pub_rank >= 3: label += " (Diverge)"
        
        logging.info(f"{label:<15} | {bets:<5} | {wins:<5} | {win_pct:>5.1f}% | {roi:>6.2f}%")
        
    overall_roi = ((total_returned - (total_bets * 100.0)) / (total_bets * 100.0)) * 100
    logging.info("-" * 55)
    logging.info(f"{'OVERALL':<15} | {total_bets:<5} | {int(top_picks[top_picks['public_rank'] <= 6]['plc_num'].eq(1).sum()):<5} | {overall_roi:>6.2f}%")

if __name__ == "__main__":
    run_divergence_hunter()