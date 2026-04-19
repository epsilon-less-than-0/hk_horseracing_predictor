#import libraries
from bs4 import BeautifulSoup
import requests
import string
import shutil
import re
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException
import pandas as pd
import os.path
import time
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraping_log.txt'),
        logging.StreamHandler()
    ]
)

# Starting URL
BASE_URL = "https://racing.hkjc.com/racing/information/English/racing/LocalResults.aspx?RaceDate="

# --- DYNAMIC DATE GENERATOR ---
def get_hkjc_likely_race_dates(start_date_str, end_date_str):
    """Generates a list of Weds, Sats, and Suns (formatted DD/MM/YYYY) newest to oldest."""
    start_date = datetime.strptime(start_date_str, "%d/%m/%Y")
    end_date = datetime.strptime(end_date_str, "%d/%m/%Y")
    
    date_list = []
    current_date = end_date # Start from the newest date
    
    while current_date >= start_date:
        # 2 = Wednesday, 5 = Saturday, 6 = Sunday
        if current_date.weekday() in [2, 5, 6]:
            date_list.append(current_date.strftime("%d/%m/%Y"))
        current_date -= timedelta(days=1)
        
    return date_list

# Generate every Wed/Sat/Sun from Sept 1, 2018 to Today (April 18, 2026)
dates = get_hkjc_likely_race_dates("01/09/2018", "18/04/2026")
logging.info(f"Generated {len(dates)} potential race dates to check.")

# --- CHROMIUM INITIALIZATION ---
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--headless") # Runs silently in the background

chromium_path = shutil.which("chromium-browser") or shutil.which("chromium")
if chromium_path:
    chrome_options.binary_location = chromium_path

driver = webdriver.Chrome(options=chrome_options)
wait = WebDriverWait(driver, 15)

def page_has_races(driver):
    """Check if current page has race data"""
    try:
        race_table = driver.find_elements(By.XPATH, "//div[5]/table/tbody/tr")
        race_info = driver.find_elements(By.XPATH, "//div[4]/table")
        has_data = len(race_table) > 0 and len(race_info) > 0
        if has_data:
            logging.info(f"Found {len(race_table)} horses in Race 1")
        return has_data
    except Exception as e:
        return False

def extract_race_headers(driver):
    """Extracts Race Name, Going, and Course robustly using Regex and DOM fallbacks."""
    race_name, race_going, race_type = "", "", ""
    try:
        # Grab the entire text block of the header table
        info_text = driver.find_element(By.XPATH, "//div[4]/table").text
        
        # Parse using Regex
        rn_match = re.search(r'(RACE\s+\d+.*?(?=\n|Going|Course|$))', info_text, re.IGNORECASE)
        race_name = rn_match.group(1).strip() if rn_match else ""
        
        going_match = re.search(r'Going\s*:\s*([^\n]+)', info_text)
        race_going = going_match.group(1).strip() if going_match else ""
        
        course_match = re.search(r'Course\s*:\s*([^\n]+)', info_text)
        race_type = course_match.group(1).strip() if course_match else ""
        
        # DOM Fallbacks (uses inner-text matching '.' instead of strict text())
        if not race_name:
            try: race_name = driver.find_element(By.XPATH, "//div[4]/table/thead/tr/td[1]").text
            except: pass
        if not race_going:
            try: race_going = driver.find_element(By.XPATH, "//td[contains(., 'Going')]/following-sibling::td").text
            except: pass
        if not race_type:
            try: race_type = driver.find_element(By.XPATH, "//td[contains(., 'Course')]/following-sibling::td").text
            except: pass
            
    except Exception as e:
        logging.warning(f"Header extraction failed: {e}")
        
    return race_name, race_going, race_type

def save_progress(processed_dates, filename="progress.txt"):
    with open(filename, 'w') as f:
        for date in processed_dates:
            f.write(date + '\n')

def load_progress(filename="progress.txt"):
    if not os.path.exists(filename):
        return []
    with open(filename, 'r') as f:
        return [line.strip() for line in f.readlines()]

# Selectors
same_day_race_link_xpaths = "//div[2]/table/tbody/tr/td/a"
table_row_xpath = "//div[5]/table/tbody/tr"

# Load progress
processed_dates = load_progress()
logging.info(f"Found {len(processed_dates)} previously processed dates")

count = 0
successful_scrapes = 0
skipped_dates = 0

# Begin Scraping
for meet in dates:
    if meet in processed_dates:
        logging.info(f"Skipping {meet} - already processed")
        continue
        
    logging.info(f"Checking: {meet}")
    race_entry = []
    count += 1
    
    if os.path.isfile(f'races{count}.csv'):
        logging.info(f"CSV file races{count}.csv already exists - skipping")
        processed_dates.append(meet)
        continue
    
    try:
        driver.set_page_load_timeout(30)
        driver.get(BASE_URL + meet)
        driver.implicitly_wait(20)
        
        if not page_has_races(driver):
            logging.info(f"No races found for {meet} - skipping")
            skipped_dates += 1
            processed_dates.append(meet)
            continue
        
        logging.info(f"Scraping: {meet}")
        
        # Get additional race links for the same day
        same_day_selel = driver.find_elements(By.XPATH, same_day_race_link_xpaths)
        same_day_links = []
        for x in same_day_selel:
            href = x.get_attribute("href")
            # Strictly filter for URLs that point to a specific race number
            if href and "RaceNo=" in href:
                same_day_links.append(href)
        
        # Scrape every race for this date
        all_race_urls = [driver.current_url] + same_day_links
        
        for url in all_race_urls:
            if url != driver.current_url:
                driver.get(url)
                time.sleep(1) # Respect server rate limiting
            
            # Use the new robust extraction function
            race_name, race_going, race_type = extract_race_headers(driver)
            
            # Extract table data
            table_rows = driver.find_elements(By.XPATH, table_row_xpath)
            for row in table_rows:
                rowEntry = [race_name, race_going, race_type]
                cols = row.find_elements(By.TAG_NAME, 'td')
                rowEntry.extend([col.text for col in cols])
                race_entry.append(rowEntry)
            
            logging.info(f"Extracted race: {race_name}")

        # Save to CSV
        if race_entry:
            df = pd.DataFrame(race_entry)
            csv_filename = f"./races{count}.csv"
            df.to_csv(csv_filename, index=False)
            logging.info(f"Saved {csv_filename} with {len(race_entry)} entries")
            successful_scrapes += 1
        
        processed_dates.append(meet)
        if count % 5 == 0:
            save_progress(processed_dates)
        
        time.sleep(2) # Throttle between dates
        
    except Exception as e:
        logging.error(f"Error processing {meet}: {e}")
        processed_dates.append(meet)
        continue

save_progress(processed_dates)
logging.info("="*50)
logging.info("SCRAPING COMPLETE")
logging.info(f"Dates checked: {len(dates)} | Successful: {successful_scrapes} | Skipped: {skipped_dates}")
logging.info("="*50)

driver.quit()