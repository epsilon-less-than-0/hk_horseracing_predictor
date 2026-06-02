"""
HKJC Unified Scraper v2.1 — for v32 Walk-Forward Backtest
==========================================================
Patches over v2.0:
  - FIX: Pool name collision. "PLACE" was matching against "QUINELLA PLACE"
         rows due to substring matching. Now matches longest pool name first.
  - ADD: race_no column in dividends.csv and metadata.csv, extracted from
         the page URL. Provides a robust integer join key independent of
         race_name extraction success.

Outputs per meeting:
  - races{N}.csv      — horse-level results (unchanged schema)
  - dividends{N}.csv  — race-level dividend payouts
       columns: date, race_no, race_name, pool, combo, dividend
  - metadata{N}.csv   — race-level context
       columns: date, race_no, race_name, going, course, distance,
                race_class, prize, url

Date range: 01/09/2011 → present.
"""

import os
import re
import time
import shutil
import logging
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
BASE_URL = "https://racing.hkjc.com/racing/information/English/racing/LocalResults.aspx?RaceDate="

START_DATE = "01/09/2011"
END_DATE   = datetime.now().strftime("%d/%m/%Y")



OUTPUT_DIR = "data/raw_csvs"   # adjust if running from a different cwd
os.makedirs(OUTPUT_DIR, exist_ok=True)

PROGRESS_FILE = os.path.join(OUTPUT_DIR, "progress.txt")
FAILED_LOG    = os.path.join(OUTPUT_DIR, "failed_extractions.txt")

# Pools to capture, ordered LONGEST FIRST so substring matching resolves
# correctly. e.g. "QUINELLA PLACE" must be tested before "PLACE", and
# "FIRST 4" before any shorter prefix.
TARGET_POOLS = [
    "QUINELLA PLACE",
    "QUINELLA",
    "TIERCE",
    "QUARTET",
    "FIRST 4",
    "TRIO",
    "PLACE",
    "WIN",
]

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(OUTPUT_DIR, 'scraping_log.txt')),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Date generator
# ---------------------------------------------------------------------
def get_hkjc_likely_race_dates(start_date_str: str, end_date_str: str) -> list:
    start = datetime.strptime(start_date_str, "%d/%m/%Y")
    end   = datetime.strptime(end_date_str,   "%d/%m/%Y")
    dates = []
    cur = end
    while cur >= start:
        if cur.weekday() in (2, 5, 6):
            dates.append(cur.strftime("%d/%m/%Y"))
        cur -= timedelta(days=1)
    return dates


# ---------------------------------------------------------------------
# Selenium driver
# ---------------------------------------------------------------------
def init_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--headless")
    chromium_path = shutil.which("chromium-browser") or shutil.which("chromium")
    if chromium_path:
        opts.binary_location = chromium_path
    return webdriver.Chrome(options=opts)


# ---------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------
def race_no_from_url(url: str) -> str:
    """Extract RaceNo query parameter. Returns '1' if absent (first race
    on the card is the default landing page with no RaceNo param)."""
    try:
        q = parse_qs(urlparse(url).query)
        rn = q.get("RaceNo", ["1"])[0]
        return str(int(rn))
    except Exception:
        return "1"


# ---------------------------------------------------------------------
# Page parsers
# ---------------------------------------------------------------------
def page_has_races(driver) -> bool:
    try:
        rows = driver.find_elements(By.XPATH, "//div[5]/table/tbody/tr")
        info = driver.find_elements(By.XPATH, "//div[4]/table")
        return len(rows) > 0 and len(info) > 0
    except Exception:
        return False


def extract_race_headers(driver) -> dict:
    out = {"race_name": "", "going": "", "course": "",
           "distance": "", "race_class": "", "prize": ""}
    try:
        info_text = driver.find_element(By.XPATH, "//div[4]/table").text

        m = re.search(r'(RACE\s+\d+.*?(?=\n|Going|Course|$))', info_text, re.I)
        if m: out["race_name"] = m.group(1).strip()

        m = re.search(r'Going\s*:\s*([^\n]+)', info_text)
        if m: out["going"] = m.group(1).strip()

        m = re.search(r'Course\s*:\s*([^\n]+)', info_text)
        if m: out["course"] = m.group(1).strip()

        m = re.search(r'(\d{3,4})\s*M\b', info_text, re.I)
        if m: out["distance"] = m.group(1)

        m = re.search(r'(Class\s+\d+|Group\s+\d+|Griffin|Restricted)',
                      info_text, re.I)
        if m: out["race_class"] = m.group(1).strip()

        m = re.search(r'HK\$?\s*([\d,]+)', info_text)
        if m: out["prize"] = m.group(1).replace(",", "")
    except Exception as e:
        log.warning(f"Header extraction failed: {e}")
    return out


def match_pool(pool_cell_text: str) -> str | None:
    """Match against TARGET_POOLS in longest-first order so that
    'QUINELLA PLACE' is not mis-matched as 'PLACE'."""
    s = pool_cell_text.upper().strip()
    for p in TARGET_POOLS:
        if p in s:
            return p
    return None


def extract_dividends(driver) -> list:
    """Parse dividend table rows.

    HKJC dividend tables: class='table_bd', pool name in first cell,
    winning combo in middle cell(s), HKD payout per unit in last cell.
    """
    out = []
    try:
        tables = driver.find_elements(By.CSS_SELECTOR, "table.table_bd")
        for table in tables:
            txt = table.text.upper()
            if not any(p in txt for p in TARGET_POOLS):
                continue
            rows = table.find_elements(By.TAG_NAME, "tr")
            for tr in rows:
                cells = tr.find_elements(By.TAG_NAME, "td")
                if len(cells) < 2:
                    continue
                matched = match_pool(cells[0].text)
                if not matched:
                    continue
                combo = cells[1].text.strip() if len(cells) >= 3 else ""
                raw   = cells[-1].text.strip()
                m = re.search(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?', raw)
                if not m:
                    continue
                out.append({
                    "pool":     matched,
                    "combo":    combo,
                    "dividend": float(m.group(0).replace(",", ""))
                })
    except Exception as e:
        log.warning(f"Dividend extraction failed: {e}")
    return out


# ---------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------
def save_progress(dates: list) -> None:
    with open(PROGRESS_FILE, 'w') as f:
        f.write("\n".join(dates))


def load_progress() -> list:
    if not os.path.exists(PROGRESS_FILE):
        return []
    with open(PROGRESS_FILE, 'r') as f:
        return [ln.strip() for ln in f if ln.strip()]


def log_failure(meet: str, race_url: str, reason: str) -> None:
    with open(FAILED_LOG, 'a') as f:
        f.write(f"{meet}\t{race_url}\t{reason}\n")


# ---------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------
def run():
    dates = get_hkjc_likely_race_dates(START_DATE, END_DATE)
    log.info(f"Generated {len(dates)} candidate race dates "
             f"({START_DATE} → {END_DATE})")

    processed = load_progress()
    log.info(f"Resume: {len(processed)} dates already processed.")

    driver = init_driver()
    wait   = WebDriverWait(driver, 15)

    count = 0
    ok    = 0
    skip  = 0

    same_day_xpath = "//div[2]/table/tbody/tr/td/a"
    row_xpath      = "//div[5]/table/tbody/tr"

    try:
        for meet in dates:
            if meet in processed:
                continue
            count += 1
            races_csv     = os.path.join(OUTPUT_DIR, f"races{count}.csv")
            dividends_csv = os.path.join(OUTPUT_DIR, f"dividends{count}.csv")
            metadata_csv  = os.path.join(OUTPUT_DIR, f"metadata{count}.csv")

            if os.path.isfile(races_csv):
                log.info(f"{meet}: races{count}.csv exists, skipping.")
                processed.append(meet)
                continue

            log.info(f"Checking {meet} ...")
            try:
                driver.set_page_load_timeout(30)
                driver.get(BASE_URL + meet)
                driver.implicitly_wait(20)

                if not page_has_races(driver):
                    log.info(f"  no races for {meet}")
                    skip += 1
                    processed.append(meet)
                    continue

                same_day = driver.find_elements(By.XPATH, same_day_xpath)
                same_day_links = [
                    x.get_attribute("href") for x in same_day
                    if x.get_attribute("href") and "RaceNo=" in x.get_attribute("href")
                ]
                all_urls = [driver.current_url] + same_day_links

                race_rows = []
                div_rows  = []
                meta_rows = []

                for url in all_urls:
                    if url != driver.current_url:
                        driver.get(url)
                        time.sleep(1)

                    race_no = race_no_from_url(driver.current_url)
                    hdr     = extract_race_headers(driver)
                    divs    = extract_dividends(driver)

                    meta_rows.append({
                        "date":       meet,
                        "race_no":    race_no,
                        "race_name":  hdr["race_name"],
                        "going":      hdr["going"],
                        "course":     hdr["course"],
                        "distance":   hdr["distance"],
                        "race_class": hdr["race_class"],
                        "prize":      hdr["prize"],
                        "url":        driver.current_url,
                    })

                    rows = driver.find_elements(By.XPATH, row_xpath)
                    for r in rows:
                        entry = [hdr["race_name"], hdr["going"], hdr["course"]]
                        cols  = r.find_elements(By.TAG_NAME, "td")
                        entry.extend([c.text for c in cols])
                        race_rows.append(entry)

                    for d in divs:
                        div_rows.append({
                            "date":      meet,
                            "race_no":   race_no,
                            "race_name": hdr["race_name"],
                            "pool":      d["pool"],
                            "combo":     d["combo"],
                            "dividend":  d["dividend"],
                        })

                    if not hdr["race_name"]:
                        log_failure(meet, driver.current_url, "empty_race_name")

                    log.info(f"  R{race_no}: {hdr['race_name']} | "
                             f"{len(rows)} horses | {len(divs)} dividend rows")

                if race_rows:
                    pd.DataFrame(race_rows).to_csv(races_csv, index=False)
                if div_rows:
                    pd.DataFrame(div_rows).to_csv(dividends_csv, index=False)
                if meta_rows:
                    pd.DataFrame(meta_rows).to_csv(metadata_csv, index=False)

                ok += 1
                processed.append(meet)
                if count % 5 == 0:
                    save_progress(processed)
                time.sleep(2)

            except TimeoutException:
                log.error(f"  timeout on {meet}")
                log_failure(meet, BASE_URL + meet, "timeout")
                processed.append(meet)
            except Exception as e:
                log.error(f"  error on {meet}: {e}")
                log_failure(meet, BASE_URL + meet, f"exception: {e}")
                processed.append(meet)

    finally:
        save_progress(processed)
        driver.quit()
        log.info("=" * 60)
        log.info(f"SCRAPE COMPLETE | checked={len(dates)} "
                 f"successful={ok} skipped={skip}")
        log.info("=" * 60)


if __name__ == "__main__":
    run()