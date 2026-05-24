import sqlite3
import pandas as pd
import xgboost as xgb
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def run_v3_divergence_hunter():
    logging.info("=== PHASE 8.3: THE DIVERGENCE HUNTER (V3 PACE TOPOLOGY) ===")
    
    # 1. Load the V3 Pace-Adjusted Model
    try:
        ranker = xgb.XGBRanker()
        ranker.load_model('v3_hkjc_ranker.json')
    except Exception as e:
        logging.error("Could not load V3 model.")
        return

    # 2. Extract Data 
    conn = sqlite3.connect('hk_racing.db')
    df_features = pd.read_sql("SELECT * FROM ml_features", conn)
    df_odds = pd.read_sql("SELECT date, race_id, horse_id, public_odds as win_odds FROM race_results", conn)
    conn.close()

    # Mathematically normalize dates and merge
    df_features['date'] = pd.to_datetime(df_features['date']).dt.normalize()
    df_odds['date'] = pd.to_datetime(df_odds['date']).dt.normalize()
    df = pd.merge(df_features, df_odds, on=['date', 'race_id', 'horse_id'], how='inner')

    # Strip ghost rows generated from previous CSV scraping overlaps
    df = df.drop_duplicates(subset=['date', 'race_id', 'horse_id'])

    df['plc_num'] = pd.to_numeric(df['plc'], errors='coerce')
    df['win_odds'] = pd.to_numeric(df['win_odds'], errors='coerce')
    
    # NEW: V3 Feature Set
    features = [
        'pre_race_elo', 'days_since_last_run', 'weight_delta', 
        'distance_delta', 'career_wins', 'is_turf', 
        'draw', 'jockey_win_pct', 'trainer_win_pct',
        'shifted_rolling_ESI', 'shifted_rolling_CSI', 
        'race_ESI_pressure', 'pace_advantage'
    ]
    
    # Filter for Unseen Holdout Data (2025+) and drop NaNs (require pace history)
    test_df = df[(df['date'].dt.year >= 2025)].dropna(subset=['plc_num', 'win_odds'] + features).copy()
    test_df = test_df.sort_values(by=['date', 'race_id'])
    
    X_test = test_df[features].astype(float)
    
    # 3. Generate AI Rankings AND Public Rankings
    logging.info(f"Scanning {len(test_df)} pace-adjusted runners for Market Divergence...")
    if len(test_df) == 0: return
        
    # AI Rank
    test_df['raw_score'] = ranker.predict(X_test)
    test_df['model_rank'] = test_df.groupby(['date', 'race_id'])['raw_score'].rank(ascending=False, method='min')
    
    # Public Rank (Lowest Odds = Rank 1)
    test_df['public_rank'] = test_df.groupby(['date', 'race_id'])['win_odds'].rank(ascending=True, method='min')
    
    # 4. Isolate Model's #1 Picks
    top_picks = test_df[test_df['model_rank'] == 1.0].copy()
    
    logging.info("\n=== V3 DIVERGENCE ANALYSIS (AI #1 PICKS) ===")
    logging.info(f"{'Public Rank':<16} | {'Bets':<5} | {'Wins':<5} | {'Win %':<6} | {'ROI':<8}")
    logging.info("-" * 56)
    
    total_bets = 0
    total_returned = 0.0
    
    # We look at the top 6 public ranks
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
        
        # Labeling
        label = f"Rank {pub_rank}"
        if pub_rank == 1: label += " (Agree)"
        elif pub_rank >= 3: label += " (Diverge)"
        
        logging.info(f"{label:<16} | {bets:<5} | {wins:<5} | {win_pct:>5.1f}% | {roi:>6.2f}%")
        
    overall_roi = ((total_returned - (total_bets * 100.0)) / (total_bets * 100.0)) * 100
    logging.info("-" * 56)
    logging.info(f"{'OVERALL':<16} | {total_bets:<5} | {int(top_picks[top_picks['public_rank'] <= 6]['plc_num'].eq(1).sum()):<5} | {overall_roi:>6.2f}%")

if __name__ == "__main__":
    run_v3_divergence_hunter()