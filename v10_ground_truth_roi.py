import sqlite3
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def scrape_quinellas():
    conn = sqlite3.connect('hk_racing.db')
    races = pd.read_sql("SELECT DISTINCT race_id, date FROM race_results WHERE date >= '2023-01-01'", conn)
    conn.execute("CREATE TABLE IF NOT EXISTS quinella_payouts (race_id TEXT PRIMARY KEY, dividend REAL)")
    
    logging.info(f"Starting armored scrape for {len(races)} races...\n")

    # The Disguise: Standard Chrome Browser Headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive'
    }

    for idx, row in races.iterrows():
        race_id = row['race_id']
        exists = conn.execute("SELECT 1 FROM quinella_payouts WHERE race_id=?", (race_id,)).fetchone()
        if exists: continue

        # Format: e.g., 2023/01/01 and Race 1
        date_str = row['date'].replace('-', '/')
        race_no = race_id[-1] if race_id[-1].isdigit() else race_id[-2:] # Handle race 10, 11
        
        url = f"https://racing.hkjc.com/racing/information/English/Racing/LocalResults.aspx?RaceDate={date_str}&RaceNo={race_no}"
        
        try:
            res = requests.get(url, headers=headers, timeout=10)
            
            # --- DIAGNOSTIC CHECK ---
            if res.status_code != 200:
                logging.warning(f"[{idx}/{len(races)}] HTTP {res.status_code} on Race {race_id} (Blocked or Missing)")
                time.sleep(2)
                continue
                
            soup = BeautifulSoup(res.text, 'html.parser')
            dividend = None
            
            # Target the Dividend Table
            tables = soup.find_all('table', class_='table_bd')
            
            if not tables:
                logging.warning(f"[{idx}/{len(races)}] Page loaded, but no dividend tables found for {race_id}. (Possible track change/abandoned race)")
            
            for table in tables:
                if 'Quinella' in table.text:
                    rows = table.find_all('tr')
                    for tr in rows:
                        cells = tr.find_all('td')
                        if len(cells) > 2 and 'Quinella' == cells[0].text.strip():
                            raw_div = cells[-1].text.strip().replace(',', '')
                            if raw_div.replace('.', '', 1).isdigit():
                                dividend = float(raw_div)
                            break
            
            if dividend:
                conn.execute("INSERT INTO quinella_payouts (race_id, dividend) VALUES (?, ?)", (race_id, dividend))
                conn.commit()
                logging.info(f"[{idx}/{len(races)}] SUCCESS: Race {race_id} Quinella Paid ${dividend:,.2f}")
            else:
                logging.info(f"[{idx}/{len(races)}] FAILED to parse Quinella string for {race_id}.")
            
            # Throttle to stay under the radar
            time.sleep(1.5) 
            
        except requests.exceptions.Timeout:
            logging.error(f"[{idx}/{len(races)}] TIMEOUT on race {race_id}.")
            time.sleep(5)
        except Exception as e:
            logging.error(f"[{idx}/{len(races)}] ERROR on race {race_id}: {e}")
            time.sleep(2)

    conn.close()
    logging.info("\nScrape Complete.")

if __name__ == "__main__":
    scrape_quinellas()