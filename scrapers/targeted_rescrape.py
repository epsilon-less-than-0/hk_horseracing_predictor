"""
HKJC Targeted Re-Scraper v1
============================
Re-scrapes a hardcoded list of problem meetings identified during the
Phase 55.1 post-scrape audit. Strategy A: full meeting delete-and-replace.

Targets:
  - 23/03/2019 (meeting 1124): truncated mid-scrape, only 5 of ~10 races
    captured, no dividends file. Full re-fetch required.
  - 13/11/2024 (meeting ???):  races 8 and 9 missing from dividends.
    Re-fetching the full meeting for cleanliness.
  - 21/09/2025 (meeting ???):  races 9 and 10 missing from dividends.
    Re-fetching the full meeting for cleanliness.

Run from project root: python3 scrapers/targeted_rescrape.py
"""

import os
import re
import time
import shutil
import logging
import glob
from datetime import datetime
from urllib.parse import urlparse, parse_qs

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

# ---------------------------------------------------------------------
# Configuration — copied from race_data_scraper_v2_1.py for consistency
# ---------------------------------------------------------------------
BASE_URL = "https://racing.hkjc.com/racing/information/English/racing/LocalResults.aspx?RaceDate="
OUTPUT_DIR = "data/raw_csvs"

TARGET_DATES = [
    "23/03/2019",
    "13/11/2024",
    "21/09/2025",
]

TARGET_POOLS = [
    "QUINELLA PLACE", "QUINELLA", "TIERCE", "QUARTET",
    "FIRST 4", "TRIO", "PLACE", "WIN",
]

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(OUTPUT_DIR, 'targeted_rescrape_log.txt')),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Locate which meeting number corresponds to each target date
# ---------------------------------------------------------------------
def find_meeting_number(target_date: str) -> int | None:
    """Search all metadata*.csv to find which meeting{N} contains the date."""
    for f in sorted(glob.glob(os.path.join(OUTPUT_DIR, 'metadata*.csv'))):
        try:
            with open(f) as fp:
                next(fp)  # skip header
                first_data = next(fp, None)
                if first_data and first_data.startswith(target_date + ','):
                    m = re.search(r'metadata(\d+)\.csv', f)
                    return int(m.group(1)) if m else None
        except Exception:
            continue
    return None


# ---------------------------------------------------------------------
# Driver + parsers — duplicated from main scraper to keep this standalone
# ---------------------------------------------------------------------
def init_driver():
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--headless")
    chromium_path = shutil.which("chromium-browser") or shutil.which("chromium")
    if chromium_path:
        opts.binary_location = chromium_path
    return webdriver.Chrome(options=opts)


def page_has_races(driver) -> bool:
    try:
        rows = driver.find_elements(By.XPATH, "//div[5]/table/tbody/tr")
        info = driver.find_elements(By.XPATH, "//div[4]/table")
        return len(rows) > 0 and len(info) > 0
    except Exception:
        return False


def race_no_from_url(url: str) -> str:
    try:
        q = parse_qs(urlparse(url).query)
        rn = q.get("RaceNo", ["1"])[0]
        return str(int(rn))
    except Exception:
        return "1"


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
        m = re.search(r'(Class\s+\d+|Group\s+\d+|Griffin|Restricted)', info_text, re.I)
        if m: out["race_class"] = m.group(1).strip()
        m = re.search(r'HK\$?\s*([\d,]+)', info_text)
        if m: out["prize"] = m.group(1).replace(",", "")
    except Exception as e:
        log.warning(f"Header extraction failed: {e}")
    return out


def match_pool(pool_cell_text: str):
    s = pool_cell_text.upper().strip()
    for p in TARGET_POOLS:
        if p in s:
            return p
    return None


def extract_dividends(driver) -> list:
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
                raw = cells[-1].text.strip()
                m = re.search(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?', raw)
                if not m:
                    continue
                out.append({"pool": matched, "combo": combo,
                            "dividend": float(m.group(0).replace(",", ""))})
    except Exception as e:
        log.warning(f"Dividend extraction failed: {e}")
    return out


# ---------------------------------------------------------------------
# Main re-scrape routine for a single meeting
# ---------------------------------------------------------------------
def rescrape_meeting(driver, meet_date: str, meeting_num: int):
    """Re-scrape a single meeting end-to-end, then overwrite its CSVs."""
    log.info(f"--- Re-scraping {meet_date} (meeting #{meeting_num}) ---")

    driver.set_page_load_timeout(45)
    driver.get(BASE_URL + meet_date)
    driver.implicitly_wait(20)
    time.sleep(2)

    if not page_has_races(driver):
        log.error(f"  {meet_date}: page reports no races — aborting this meeting")
        return False

    same_day = driver.find_elements(By.XPATH, "//div[2]/table/tbody/tr/td/a")
    same_day_links = [
        x.get_attribute("href") for x in same_day
        if x.get_attribute("href") and "RaceNo=" in x.get_attribute("href")
    ]
    all_urls = [driver.current_url] + same_day_links
    log.info(f"  {meet_date}: {len(all_urls)} race URLs to fetch")

    race_rows = []
    div_rows  = []
    meta_rows = []

    for url in all_urls:
        if url != driver.current_url:
            driver.get(url)
            time.sleep(1.5)

        race_no = race_no_from_url(driver.current_url)
        hdr     = extract_race_headers(driver)
        divs    = extract_dividends(driver)

        meta_rows.append({
            "date":       meet_date,
            "race_no":    race_no,
            "race_name":  hdr["race_name"],
            "going":      hdr["going"],
            "course":     hdr["course"],
            "distance":   hdr["distance"],
            "race_class": hdr["race_class"],
            "prize":      hdr["prize"],
            "url":        driver.current_url,
        })

        rows = driver.find_elements(By.XPATH, "//div[5]/table/tbody/tr")
        for r in rows:
            entry = [hdr["race_name"], hdr["going"], hdr["course"]]
            cols = r.find_elements(By.TAG_NAME, "td")
            entry.extend([c.text for c in cols])
            race_rows.append(entry)

        for d in divs:
            div_rows.append({
                "date":      meet_date,
                "race_no":   race_no,
                "race_name": hdr["race_name"],
                "pool":      d["pool"],
                "combo":     d["combo"],
                "dividend":  d["dividend"],
            })

        log.info(f"    R{race_no}: {len(rows)} horses, {len(divs)} dividend rows")

    # Sanity check before overwriting
    if len(meta_rows) < 5:
        log.error(f"  {meet_date}: only got {len(meta_rows)} races — refusing to "
                  f"overwrite existing files. Investigate manually.")
        return False

    races_csv     = os.path.join(OUTPUT_DIR, f"races{meeting_num}.csv")
    dividends_csv = os.path.join(OUTPUT_DIR, f"dividends{meeting_num}.csv")
    metadata_csv  = os.path.join(OUTPUT_DIR, f"metadata{meeting_num}.csv")

    pd.DataFrame(race_rows).to_csv(races_csv, index=False)
    pd.DataFrame(div_rows).to_csv(dividends_csv, index=False)
    pd.DataFrame(meta_rows).to_csv(metadata_csv, index=False)

    log.info(f"  {meet_date}: wrote {len(race_rows)} race rows, "
             f"{len(div_rows)} dividend rows, {len(meta_rows)} metadata rows")
    return True


# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------
def run():
    log.info("=" * 60)
    log.info("Phase 55.1 targeted re-scrape starting")
    log.info(f"Targets: {TARGET_DATES}")
    log.info("=" * 60)

    # Resolve meeting numbers from existing metadata files
    targets = []
    for d in TARGET_DATES:
        n = find_meeting_number(d)
        if n is None:
            log.error(f"Could not locate existing meeting number for {d} — "
                      f"will need manual handling. Skipping.")
            continue
        targets.append((d, n))
        log.info(f"  {d} → meeting #{n}")

    if not targets:
        log.error("No targets resolvable. Aborting.")
        return

    driver = init_driver()
    successes = 0
    try:
        for meet_date, meeting_num in targets:
            try:
                if rescrape_meeting(driver, meet_date, meeting_num):
                    successes += 1
                time.sleep(3)
            except TimeoutException:
                log.error(f"  {meet_date}: timeout — meeting NOT updated")
            except Exception as e:
                log.error(f"  {meet_date}: error: {e} — meeting NOT updated")
    finally:
        driver.quit()
        log.info("=" * 60)
        log.info(f"Re-scrape complete: {successes}/{len(targets)} meetings updated")
        log.info("=" * 60)


if __name__ == "__main__":
    run()
