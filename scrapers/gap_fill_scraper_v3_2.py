"""
HKJC Gap-Fill Scraper v3.2 — Surgical Recovery
================================================
Targets the specific dates lost during the DNS outage of May 31, 2026
(05:40-onwards), which caused 356 meetings — concentrated in 2018-2021 —
to be marked as "processed" in progress.txt without actually capturing data.

Reads target dates from data/raw_csvs/gap_fill_dates.txt and processes
only those dates. Output files are numbered starting at MAX(existing)+1
so they merge cleanly with the existing 851 meetings.

Uses identical extraction logic to race_data_scraper_v3_2.py — the same
state-machine dividend parser, the same incident capture, the same
metadata fields. Output schemas are byte-compatible.

Run from project root: python3 scrapers/gap_fill_scraper_v3_2.py
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
# Configuration — identical to v3.2 main scraper
# ---------------------------------------------------------------------
BASE_URL = "https://racing.hkjc.com/racing/information/English/racing/LocalResults.aspx?RaceDate="

_SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
OUTPUT_DIR    = os.path.join(_PROJECT_ROOT, "data", "raw_csvs")

TARGET_LIST   = os.path.join(OUTPUT_DIR, "gap_fill_dates.txt")
PROGRESS_FILE = os.path.join(OUTPUT_DIR, "gap_fill_progress.txt")
FAILED_LOG    = os.path.join(OUTPUT_DIR, "gap_fill_failed.txt")

TARGET_POOLS = [
    "QUINELLA PLACE", "QUINELLA", "TIERCE", "QUARTET",
    "FIRST 4", "TRIO", "PLACE", "WIN",
]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(OUTPUT_DIR, 'gap_fill_log.txt')),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Determine starting file number to avoid collision with existing CSVs
# ---------------------------------------------------------------------
def find_next_meeting_number():
    """Scan existing races{N}.csv files; return MAX(N) + 1."""
    existing = []
    for f in os.listdir(OUTPUT_DIR):
        m = re.match(r'races(\d+)\.csv$', f)
        if m:
            existing.append(int(m.group(1)))
    return max(existing) + 1 if existing else 1


def load_target_dates():
    """Load the missing-dates list."""
    if not os.path.exists(TARGET_LIST):
        log.error(f"Target list not found: {TARGET_LIST}")
        return []
    with open(TARGET_LIST) as f:
        return [ln.strip() for ln in f if ln.strip()]


def load_progress():
    if not os.path.exists(PROGRESS_FILE):
        return []
    with open(PROGRESS_FILE) as f:
        return [ln.strip() for ln in f if ln.strip()]


def save_progress(dates):
    with open(PROGRESS_FILE, 'w') as f:
        f.write("\n".join(dates))


def log_failure(meet, url, reason):
    with open(FAILED_LOG, 'a') as f:
        f.write(f"{meet}\t{url}\t{reason}\n")


# ---------------------------------------------------------------------
# Selenium + parsers — identical to v3.2
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


def race_no_from_url(url):
    try:
        q = parse_qs(urlparse(url).query)
        rn = q.get("RaceNo", ["1"])[0]
        return str(int(rn))
    except Exception:
        return "1"


def is_overseas_url(url):
    return bool(url) and '/overseas/' in url.lower()


def parse_horse_id_from_text(text):
    m = re.match(r'^\s*(.+?)\s*\(([A-Z0-9]{3,5})\)\s*$', str(text))
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return str(text).strip(), ""


def page_has_races(driver):
    try:
        rows = driver.find_elements(By.XPATH, "//div[5]/table/tbody/tr")
        info = driver.find_elements(By.XPATH, "//div[4]/table")
        return len(rows) > 0 and len(info) > 0
    except Exception:
        return False


def page_is_abandoned(driver):
    try:
        return "declared abandoned" in driver.find_element(By.TAG_NAME, "body").text.lower()
    except Exception:
        return False


def extract_race_headers(driver):
    out = {"race_name":"","going":"","course":"","distance":"","race_class":"","prize":""}
    try:
        info_text = driver.find_element(By.XPATH, "//div[4]/table").text
        m = re.search(r'(RACE\s+\d+.*?(?=\n|Going|Course|$))', info_text, re.I)
        if m: out["race_name"] = m.group(1).strip()
        m = re.search(r'Going\s*:\s*([^\n]+)', info_text)
        if m: out["going"] = m.group(1).strip()
        m = re.search(r'Course\s*:\s*([^\n]+)', info_text)
        if m: out["course"] = m.group(1).strip()
        for pat in [r'(\d{1,2},?\d{3})\s*M(?:etres?)?\b',
                    r'(\d{3,4})\s*M(?:etres?)?\b',
                    r'(\d{3,4})\s*m(?:etres?)?\b']:
            m = re.search(pat, info_text)
            if m:
                out["distance"] = m.group(1).replace(",", "")
                break
        m = re.search(r'(Class\s+\d+|Group\s+\d+|Griffin|Restricted)', info_text, re.I)
        if m: out["race_class"] = m.group(1).strip()
        m = re.search(r'HK\$?\s*([\d,]+)', info_text)
        if m: out["prize"] = m.group(1).replace(",", "")
    except Exception as e:
        log.warning(f"Header extraction failed: {e}")
    return out


def match_pool(text):
    s = text.upper().strip()
    if not s: return None
    for p in TARGET_POOLS:
        if p in s: return p
    return None


def extract_dividends(driver):
    out = []
    try:
        for table in driver.find_elements(By.CSS_SELECTOR, "table.table_bd"):
            txt = table.text.upper()
            if not any(p in txt for p in TARGET_POOLS):
                continue
            last_seen_pool = None
            for tr in table.find_elements(By.TAG_NAME, "tr"):
                cells = tr.find_elements(By.TAG_NAME, "td")
                n = len(cells)
                if n >= 3:
                    cand = match_pool(cells[0].text)
                    if cand is not None:
                        last_seen_pool = cand
                        combo_idx = 1
                    else:
                        last_seen_pool = None
                        continue
                elif n == 2:
                    if last_seen_pool is None: continue
                    combo_idx = 0
                else:
                    continue
                combo = cells[combo_idx].text.strip()
                raw = cells[-1].text.strip()
                if 'REFUND' in raw.upper():
                    out.append({"pool":last_seen_pool, "combo":combo,
                                "dividend":None, "is_refund":1})
                    continue
                m = re.search(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?', raw)
                if not m: continue
                out.append({"pool":last_seen_pool, "combo":combo,
                            "dividend":float(m.group(0).replace(",","")),
                            "is_refund":0})
    except Exception as e:
        log.warning(f"Dividend extraction failed: {e}")
    return out


def extract_incidents(driver):
    out = []
    try:
        for table in driver.find_elements(By.CSS_SELECTOR, "table.table_bd"):
            txt = table.text
            first_line = txt.split('\n')[0].lower() if txt else ""
            if 'incident' not in first_line: continue
            if 'jockey' in first_line or 'dividend' in first_line: continue
            if 'pool' in first_line: continue
            for tr in table.find_elements(By.TAG_NAME, "tr"):
                cells = tr.find_elements(By.TAG_NAME, "td")
                if len(cells) < 4: continue
                placing = cells[0].text.strip()
                if not placing or not re.match(r'^\d+', placing): continue
                horse_no = cells[1].text.strip()
                horse_raw = cells[2].text.strip()
                incident_text = cells[3].text.strip()
                horse_name, horse_id = parse_horse_id_from_text(horse_raw)
                out.append({"placing":placing, "horse_no":horse_no,
                            "horse_name":horse_name, "horse_id":horse_id,
                            "incident_text":incident_text})
            break
    except Exception as e:
        log.warning(f"Incident extraction failed: {e}")
    return out


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def run():
    targets = load_target_dates()
    if not targets:
        log.error("No target dates loaded. Run the Step 1 script first.")
        return

    log.info(f"Gap-fill: {len(targets)} target dates loaded")
    processed = set(load_progress())
    log.info(f"Resume: {len(processed)} dates already processed")

    counter = find_next_meeting_number()
    log.info(f"File numbering starts at #{counter} (existing max + 1)")

    driver = init_driver()
    ok = 0

    try:
        for meet in targets:
            if meet in processed:
                continue

            races_csv     = os.path.join(OUTPUT_DIR, f"races{counter}.csv")
            dividends_csv = os.path.join(OUTPUT_DIR, f"dividends{counter}.csv")
            metadata_csv  = os.path.join(OUTPUT_DIR, f"metadata{counter}.csv")
            incidents_csv = os.path.join(OUTPUT_DIR, f"incidents{counter}.csv")

            log.info(f"Processing {meet} (file #{counter})")
            try:
                driver.set_page_load_timeout(45)
                driver.get(BASE_URL + meet)
                driver.implicitly_wait(20)
                time.sleep(2)

                if not page_has_races(driver):
                    log.warning(f"  {meet}: page reports no races")
                    log_failure(meet, BASE_URL + meet, "no_races")
                    processed.add(meet)
                    counter += 1
                    continue

                same_day = driver.find_elements(By.XPATH, "//div[2]/table/tbody/tr/td/a")
                same_day_links = []
                for x in same_day:
                    href = x.get_attribute("href")
                    if not href or "RaceNo=" not in href: continue
                    if is_overseas_url(href): continue
                    same_day_links.append(href)
                all_urls = [driver.current_url] + same_day_links

                race_rows, div_rows, meta_rows, inc_rows = [], [], [], []

                for url in all_urls:
                    if is_overseas_url(url): continue
                    if url != driver.current_url:
                        driver.get(url); time.sleep(1.5)

                    race_no = race_no_from_url(driver.current_url)
                    abandoned = page_is_abandoned(driver)
                    hdr = extract_race_headers(driver)
                    divs = extract_dividends(driver)
                    incs = extract_incidents(driver)

                    meta_rows.append({
                        "date":meet, "race_no":race_no,
                        "race_name":hdr["race_name"], "going":hdr["going"],
                        "course":hdr["course"], "distance":hdr["distance"],
                        "race_class":hdr["race_class"], "prize":hdr["prize"],
                        "url":driver.current_url,
                    })

                    for r in driver.find_elements(By.XPATH, "//div[5]/table/tbody/tr"):
                        entry = [hdr["race_name"], hdr["going"], hdr["course"]]
                        entry.extend([c.text for c in r.find_elements(By.TAG_NAME,"td")])
                        race_rows.append(entry)

                    for d in divs:
                        div_rows.append({
                            "date":meet, "race_no":race_no,
                            "race_name":hdr["race_name"], "pool":d["pool"],
                            "combo":d["combo"], "dividend":d["dividend"],
                            "is_refund":d["is_refund"],
                        })

                    for i in incs:
                        inc_rows.append({
                            "date":meet, "race_no":race_no,
                            "race_name":hdr["race_name"], "placing":i["placing"],
                            "horse_no":i["horse_no"], "horse_name":i["horse_name"],
                            "horse_id":i["horse_id"], "incident_text":i["incident_text"],
                        })

                    if not hdr["race_name"] and not abandoned:
                        log_failure(meet, driver.current_url, "empty_race_name")

                    status = "ABANDONED" if abandoned else f"{len(driver.find_elements(By.XPATH, '//div[5]/table/tbody/tr'))} horses"
                    log.info(f"  R{race_no}: {hdr['race_name']} | {status} | "
                             f"{len(divs)} divs | {len(incs)} incidents")

                if race_rows:
                    pd.DataFrame(race_rows).to_csv(races_csv, index=False)
                if div_rows:
                    pd.DataFrame(div_rows).to_csv(dividends_csv, index=False)
                if meta_rows:
                    pd.DataFrame(meta_rows).to_csv(metadata_csv, index=False)
                if inc_rows:
                    pd.DataFrame(inc_rows).to_csv(incidents_csv, index=False)

                ok += 1
                processed.add(meet)
                counter += 1

                if ok % 10 == 0:
                    save_progress(list(processed))

                time.sleep(2)

            except TimeoutException:
                log.error(f"  TIMEOUT on {meet}")
                log_failure(meet, BASE_URL + meet, "timeout")
                # Do NOT mark as processed — leave for next retry
            except Exception as e:
                log.error(f"  ERROR on {meet}: {e}")
                log_failure(meet, BASE_URL + meet, f"exception: {e}")
                # Do NOT mark as processed if it's a network issue
                err_str = str(e).lower()
                if 'name_not_resolved' in err_str or 'timed out' in err_str:
                    log.warning(f"  Network error — leaving {meet} for retry")
                else:
                    processed.add(meet)
                    counter += 1

    finally:
        save_progress(list(processed))
        driver.quit()
        log.info("=" * 60)
        log.info(f"GAP-FILL COMPLETE | targets={len(targets)} successful={ok}")
        log.info(f"  Remaining unprocessed: {len(targets) - len(processed)}")
        log.info("=" * 60)


if __name__ == "__main__":
    run()
