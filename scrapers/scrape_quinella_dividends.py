import sqlite3
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
import re

logging.basicConfig(level=logging.INFO, format='%(message)s')

def scrape_quinellas():
    conn = sqlite3.connect('hk_racing.db')
    
    races = pd.read_sql("SELECT DISTINCT race_id, date, race_no FROM race_results WHERE date >= '2023-01-01'", conn)
    conn.execute("CREATE TABLE IF NOT EXISTS quinella_payouts (race_id TEXT PRIMARY KEY, dividend REAL)")
    
    logging.info(f"Starting precision scrape for {len(races)} races...\n")

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

        date_str = row['date'].replace('-', '/')
        
        # FIX 4: Strip out 'RACE_' strings. Extract ONLY the integer.
        raw_race_no = str(row['race_no'])
        race_no = re.sub(r'\D', '', raw_race_no) 
        
        url = f"https://racing.hkjc.com/racing/information/English/Racing/LocalResults.aspx?RaceDate={date_str}&RaceNo={race_no}"
        
        try:
            res = requests.get(url, headers=headers, timeout=10)
            
            if res.status_code != 200:
                logging.warning(f"[{idx}/{len(races)}] HTTP {res.status_code} on Race {race_id}")
                time.sleep(2)
                continue
                
            soup = BeautifulSoup(res.text, 'html.parser')
            dividend = None
            
            tables = soup.find_all('table', class_='table_bd')
            
            for table in tables:
                if 'QUIN' in table.text.upper():
                    rows = table.find_all('tr')
                    for tr in rows:
                        cells = tr.find_all('td')
                        if len(cells) > 2 and 'QUIN' in cells[0].text.strip().upper():
                            raw_div = cells[-1].text.strip()
                            match = re.search(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?', raw_div)
                            if match:
                                dividend = float(match.group(0).replace(',', ''))
                            break
            
            if dividend:
                conn.execute("INSERT INTO quinella_payouts (race_id, dividend) VALUES (?, ?)", (race_id, dividend))
                conn.commit()
                logging.info(f"[{idx}/{len(races)}] SUCCESS: Race {race_id} (Date: {date_str}, No: {race_no}) Paid ${dividend:,.2f}")
            else:
                logging.warning(f"[{idx}/{len(races)}] FAILED: No payout found on page -> {url}")
            
            time.sleep(1.5) 
            
        except Exception as e:
            logging.error(f"[{idx}/{len(races)}] ERROR on race {race_id}: {e}")
            time.sleep(2)

    conn.close()
    logging.info("\nScrape Complete.")

if __name__ == "__main__":
    scrape_quinellas()