#import libraries
from bs4 import BeautifulSoup
import requests
import string
from selenium import webdriver
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

#starting webdriver
BASE_URL = "https://racing.hkjc.com/racing/information/English/racing/LocalResults.aspx?RaceDate="

# TEST DATES - Just a few dates to test with (mix of likely race days and non-race days)
dates = ["29/12/2019", "28/12/2019", "27/12/2019", "26/12/2019", "25/12/2019", 
         "24/12/2019", "23/12/2019", "22/12/2019", "21/12/2019", "20/12/2019"]

# UNCOMMENT BELOW FOR FULL DATE RANGE LATER:
# dates = ["29/12/2019", "26/12/2019", "21/12/2019", "18/12/2019", "15/12/2019", "11/12/2019", "08/12/2019",
#  "04/12/2019", "01/12/2019", "27/11/2019", "23/11/2019", "20/11/2019", "17/11/2019", "09/11/2019",
#   "06/11/2019", "03/11/2019", "30/10/2019", "27/10/2019", "23/10/2019", "20/10/2019", "16/10/2019",
#    "12/10/2019", "09/10/2019", "01/10/2019", "25/09/2019", "21/09/2019", "15/09/2019", "11/09/2019",
#      "01/09/2019", "14/07/2019", "10/07/2019", "07/07/2019", "03/07/2019", "01/07/2019", "26/06/2019", "23/06/2019",
#       "16/06/2019", "05/06/2019", "29/05/2019", "22/05/2019", "15/05/2019", "11/05/2019", "08/05/2019", "05/05/2019",
#        "01/05/2019", "28/04/2019", "22/04/2019",  "07/04/2019","03/04/2019", "31/03/2019", "24/03/2019", "23/03/2019", "20/03/2019",
#         "17/03/2019", "13/03/2019", "10/03/2019", "06/03/2019", "02/03/2019", "27/02/2019",
#          "24/02/2019", "17/02/2019", "13/02/2019","10/02/2019", "07/02/2019", "02/02/2019", "30/01/2019", "27/01/2019",
#           "23/01/2019", "20/01/2019", "16/01/2019", "12/01/2019", "09/01/2019", "06/01/2019", "01/01/2019",
#            "29/12/2018", "26/12/2018", "23/12/2018", "19/12/2018", "16/12/2018", "12/12/2018", "09/12/2018",
#             "05/12/2018", "02/12/2018", "28/11/2018", "25/11/2018", "21/11/2018", "18/11/2018", "14/11/2018", "10/11/2018", "07/11/2018",
#              "04/11/2018", "31/10/2018", "28/10/2018", "24/10/2018", "21/10/2018",  "18/10/2018",
#               "13/10/2018", "10/10/2018", "01/10/2018", "26/09/2018", "22/09/2018", "12/09/2018",
#                "09/09/2018", "05/09/2018", "02/09/2018"]

driver = webdriver.Firefox()
wait = WebDriverWait(driver, 15)

def check_exists_by_xpath(xpath):
    try:
        driver.find_element_by_xpath(xpath)
    except NoSuchElementException:
        return False
    return True

def page_has_races(driver):
    """Check if current page has race data"""
    try:
        # Check for the race table with actual data rows
        race_table = driver.find_elements_by_xpath("//div[5]/table/tbody/tr")
        
        # Also check if there's race information header
        race_info = driver.find_elements_by_xpath("//div[4]/table")
        
        has_data = len(race_table) > 0 and len(race_info) > 0
        
        if has_data:
            logging.info(f"Found {len(race_table)} race entries")
        
        return has_data
        
    except Exception as e:
        logging.warning(f"Error checking for race data: {e}")
        return False

def save_progress(processed_dates, filename="progress.txt"):
    """Save list of processed dates to file"""
    with open(filename, 'w') as f:
        for date in processed_dates:
            f.write(date + '\n')

def load_progress(filename="progress.txt"):
    """Load list of processed dates from file"""
    if not os.path.exists(filename):
        return []
    
    with open(filename, 'r') as f:
        return [line.strip() for line in f.readlines()]

"""
Initialize variables: 

Data collected per entry: 
  place, horse_no, horse, jockey, trainer, actual_wt,
  declare_horse_wt, draw, lbw, running_pos, finish_time, win_odds
"""

race_name_xpath = "/html/body/div/div[4]/table/thead/tr/td[1]"
race_type_xpath = "/html/body/div/div[4]/table/tbody/tr[2]/td[1]"
race_going_xpath = "/html/body/div/div[4]/table/tbody/tr[2]/td[3]"
race_table_xpath = string.Template('''/html/body/div/div[5]/table/tbody/tr[$row]/td[$col]''')

same_day_race_link_xpaths = "//div[2]/table/tbody/tr/td/a"
table_row_xpath = "//div[5]/table/tbody/tr"

# Load previously processed dates
processed_dates = load_progress()
logging.info(f"Found {len(processed_dates)} previously processed dates")

count = 0
race_name = ""
race_going = ""
race_type = ""
successful_scrapes = 0
skipped_dates = 0

# Begin grabbing data
for meet in dates:
    # Skip if already processed
    if meet in processed_dates:
        logging.info(f"Skipping {meet} - already processed")
        continue
        
    logging.info(f"Checking: {meet}")
    
    race_entry = []
    internalRaceCount = 1
    count += 1
    
    # Check if CSV already exists
    if os.path.isfile('races' + str(count) + '.csv'):
        logging.info(f"CSV file races{count}.csv already exists - skipping")
        processed_dates.append(meet)
        continue
    
    try:
        driver.set_page_load_timeout(30)
        driver.get(BASE_URL + meet)
        driver.implicitly_wait(20)
        
        # Check if this page has race data
        if not page_has_races(driver):
            logging.info(f"No races found for {meet} - skipping")
            skipped_dates += 1
            processed_dates.append(meet)
            continue
        
        logging.info(f"Scraping: {meet}")
        
        same_day_selel = driver.find_elements_by_xpath(same_day_race_link_xpaths)[:-1]
        same_day_links = [x.get_attribute("href") for x in same_day_selel]  
        
        # Get first race - x columns y rows + race name, going, track type
        tempTableEl = wait.until(EC.presence_of_all_elements_located((By.XPATH, table_row_xpath)))
        table_rows = tempTableEl

        if (check_exists_by_xpath(race_name_xpath)):
            tempEl = wait.until(EC.presence_of_element_located((By.XPATH, race_name_xpath)))
            race_name = (tempEl.text)
        if (check_exists_by_xpath(race_going_xpath)):
            tempEl = wait.until(EC.presence_of_element_located((By.XPATH,race_going_xpath)))
            race_going = (tempEl.text)
        if (check_exists_by_xpath(race_type_xpath)):
            tempEl = wait.until(EC.presence_of_element_located((By.XPATH,race_type_xpath)))
            race_type = (tempEl.text)

        for row in table_rows:
            rowEntry = []
            rowEntry.append(race_name)
            rowEntry.append(race_going)
            rowEntry.append(race_type)
            cols = row.find_elements_by_tag_name('td')
            for col in cols:
                rowEntry.append(col.text)
            race_entry.append(rowEntry)
        
        logging.info(f"Extracted data for first race: {race_name}")
        
        # Get other races on same meet
        for same_day_link in same_day_links:
            logging.info(f"Scraping additional race: {same_day_link}")
            internalRaceCount += 1
            driver.get(same_day_link)
            driver.implicitly_wait(10)
            
            # Be respectful to the server
            time.sleep(1)

            # Scrape 2nd - n
            if (check_exists_by_xpath(race_name_xpath)):
                tempEl = wait.until(EC.presence_of_element_located((By.XPATH, race_name_xpath)))
                race_name = (tempEl.text)
            if (check_exists_by_xpath(race_going_xpath)):
                tempEl = wait.until(EC.presence_of_element_located((By.XPATH,race_going_xpath)))
                race_going = (tempEl.text)
            if (check_exists_by_xpath(race_type_xpath)):
                tempEl = wait.until(EC.presence_of_element_located((By.XPATH,race_type_xpath)))
                race_type = (tempEl.text)

            table_rows = driver.find_elements_by_xpath(table_row_xpath)

            for row in table_rows:
                rowEntry = []
                rowEntry.append(race_name)
                rowEntry.append(race_going)
                rowEntry.append(race_type)
                cols = row.find_elements_by_tag_name('td')
                for col in cols:
                    rowEntry.append(col.text)
                race_entry.append(rowEntry)
        
        # Save file as csv
        if race_entry:  # Only save if we have data
            df = pd.DataFrame(race_entry)
            csv_filename = "./races" + str(count) + ".csv"
            df.to_csv(csv_filename, index=False)
            logging.info(f"Saved {csv_filename} with {len(race_entry)} entries from {internalRaceCount} races")
            logging.info(f"Sample data: {df.head()}")
            successful_scrapes += 1
        else:
            logging.warning(f"No data extracted for {meet}")
        
        # Mark as processed
        processed_dates.append(meet)
        
        # Save progress periodically
        if count % 5 == 0:  # Save more frequently during testing
            save_progress(processed_dates)
            logging.info(f"Progress saved. Processed {len(processed_dates)} dates so far.")
        
        # Be respectful to the server
        time.sleep(2)
        
    except Exception as e:
        logging.error(f"Error processing {meet}: {e}")
        # Still mark as processed to avoid retrying failed dates
        processed_dates.append(meet)
        continue

# Final save of progress
save_progress(processed_dates)

# Summary
logging.info("="*50)
logging.info("SCRAPING COMPLETE")
logging.info(f"Total dates checked: {len(dates)}")
logging.info(f"Dates with races: {successful_scrapes}")
logging.info(f"Dates with no races: {skipped_dates}")
logging.info(f"Total files created: {successful_scrapes}")
logging.info("="*50)

driver.quit()