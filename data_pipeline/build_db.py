import pandas as pd
import sqlite3
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# 1. Load your master ELO-AUGMENTED CSV
logging.info("Loading Elo-augmented CSV...")
try:
    # low_memory=False suppresses the mixed Dtype warning on column 15
    df = pd.read_csv('elo_augmented_race_data.csv', low_memory=False)
except FileNotFoundError:
    logging.error("elo_augmented_race_data.csv not found! Run elo_builder.py first.")
    exit()

# 2. Connect to the SQLite database 
logging.info("Connecting to hk_racing.db...")
conn = sqlite3.connect('hk_racing.db')
cursor = conn.cursor()

# 3. Push the entire dataframe into the 'race_results' table
logging.info("Building race_results table...")
df.to_sql('race_results', conn, if_exists='replace', index=False)

# 4. Create SQL Indexes for lightning-fast ML queries
logging.info("Creating database indexes...")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_horse ON race_results(horse_id);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_date ON race_results(date);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_jockey ON race_results(jockey_name);")
# NEW: Composite index to handle the HKJC's seasonal race_id reset
cursor.execute("CREATE INDEX IF NOT EXISTS idx_race_event ON race_results(date, race_id);")

# 5. Verify it worked with proper quantitative logic
cursor.execute("SELECT COUNT(DISTINCT horse_id) FROM race_results")
horse_count = cursor.fetchone()[0]

# Correctly counting unique races by treating Date + Race_ID as the composite key
cursor.execute("SELECT COUNT(*) FROM (SELECT DISTINCT date, race_id FROM race_results)")
race_count = cursor.fetchone()[0]

logging.info(f"Success! Database created with {horse_count} unique horses across {race_count} distinct races.")
conn.close()