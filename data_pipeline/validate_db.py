import sqlite3
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def validate_database():
    logging.info("=== HKJC DATABASE AUDIT ===")
    conn = sqlite3.connect('hk_racing.db')
    
    # 1. Volume Check (using composite key for races)
    races = pd.read_sql("SELECT COUNT(*) FROM (SELECT DISTINCT date, race_id FROM race_results)", conn).iloc[0,0]
    horses = pd.read_sql("SELECT COUNT(DISTINCT horse_id) FROM race_results", conn).iloc[0,0]
    entries = pd.read_sql("SELECT COUNT(*) FROM race_results", conn).iloc[0,0]
    
    logging.info(f"\n[VOLUME]")
    logging.info(f"Total Races:   {races:,}")
    logging.info(f"Total Horses:  {horses:,}")
    logging.info(f"Total Entries: {entries:,}")

    # 2. Null Value Check (Critical for ML)
    nulls = pd.read_sql("""
        SELECT 
            SUM(CASE WHEN horse_id IS NULL THEN 1 ELSE 0 END) as missing_horse_id,
            SUM(CASE WHEN plc IS NULL OR plc = 'NaN' THEN 1 ELSE 0 END) as missing_placings,
            SUM(CASE WHEN post_race_elo IS NULL THEN 1 ELSE 0 END) as missing_elo
        FROM race_results
    """, conn)
    
    logging.info(f"\n[DATA INTEGRITY]")
    logging.info(f"Missing Horse IDs: {nulls['missing_horse_id'].iloc[0]}")
    logging.info(f"Missing Placings:  {nulls['missing_placings'].iloc[0]} (Expected: Reflects scratched/DNF horses)")
    logging.info(f"Missing Elos:      {nulls['missing_elo'].iloc[0]}")

    # 3. Elo Distribution (Sanity Check)
    elo_stats = pd.read_sql("""
        SELECT 
            ROUND(AVG(post_race_elo), 2) as avg_elo,
            ROUND(MAX(post_race_elo), 2) as max_elo,
            ROUND(MIN(post_race_elo), 2) as min_elo
        FROM race_results
    """, conn)
    
    logging.info(f"\n[ELO MATHEMATICS]")
    logging.info(f"Average System Elo: {elo_stats['avg_elo'].iloc[0]} (Target: ~1500)")
    logging.info(f"Highest Peak Elo:   {elo_stats['max_elo'].iloc[0]}")
    logging.info(f"Lowest Trough Elo:  {elo_stats['min_elo'].iloc[0]}")

    # 4. The "Eye Test" (Top 5 Horses of the Decade)
    # We query the highest final Elos achieved to see if the math matches reality
    top_horses = pd.read_sql("""
        SELECT horse_name, MAX(post_race_elo) as peak_elo 
        FROM race_results 
        GROUP BY horse_id 
        ORDER BY peak_elo DESC 
        LIMIT 5
    """, conn)
    
    logging.info(f"\n[THE EYE TEST - ALL-TIME PEAK RATINGS]")
    for idx, row in top_horses.iterrows():
        logging.info(f"{idx+1}. {row['horse_name']} - {row['peak_elo']}")
        
    conn.close()

if __name__ == "__main__":
    validate_database()