"""
HKJC Unified Scraper v3.2 — for v32 Walk-Forward Backtest
==========================================================
Patches over v3.1:

  INCIDENT REPORT CAPTURE: Captures HKJC's "Racing Incident Report" table
  per race, storing raw incident text per horse. Enables future LLM-based
  structured extraction (FUTURE-1 in project log) without requiring a
  third full re-scrape.

  The incident table is HKJC's stewards' narrative — observations on each
  horse's race (traffic trouble, interference, veterinary findings, etc.).
  Text is preserved raw; downstream NLP/LLM parsing happens offline.

Outputs per meeting:
  - races{N}.csv      — horse-level results (unchanged)
  - dividends{N}.csv  — pool dividends with v3.1's proper state machine
  - metadata{N}.csv   — race-level context
  - incidents{N}.csv  — NEW: per-horse incident narratives
       columns: date, race_no, race_name, horse_no, horse_name, horse_id,
                placing, incident_text
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
# START_DATE = "10/05/2026"
END_DATE   = datetime.now().strftime("%d/%m/%Y")

_SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
OUTPUT_DIR    = os.path.join(_PROJECT_ROOT, "data", "raw_csvs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

PROGRESS_FILE = os.path.join(OUTPUT_DIR, "progress.txt")
FAILED_LOG    = os.path.join(OUTPUT_DIR, "failed_extractions.txt")

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
        logging.FileHandler(os.path.join(OUTPUT_DIR, 'scraping_log.txt')),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


def get_hkjc_likely_race_dates(start_date_str, end_date_str):
    start = datetime.strptime(start_date_str, "%d/%m/%Y")
    end   = datetime.strptime(end_date_str,   "%d/%m/%Y")
    dates = []
    cur = end
    while cur >= start:
        if cur.weekday() in (2, 5, 6):
            dates.append(cur.strftime("%d/%m/%Y"))
        cur -= timedelta(days=1)
    return dates


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
    if not url:
        return False
    return '/overseas/' in url.lower()


def parse_horse_id_from_text(text):
    """'FLYING AMANI (K152)' -> ('FLYING AMANI', 'K152')."""
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
        page_text = driver.find_element(By.TAG_NAME, "body").text
        return "declared abandoned" in page_text.lower()
    except Exception:
        return False


def extract_race_headers(driver):
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

        for pattern in [
            r'(\d{1,2},?\d{3})\s*M(?:etres?)?\b',
            r'(\d{3,4})\s*M(?:etres?)?\b',
            r'(\d{3,4})\s*m(?:etres?)?\b',
        ]:
            m = re.search(pattern, info_text)
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


def match_pool(pool_cell_text):
    s = pool_cell_text.upper().strip()
    if not s:
        return None
    for p in TARGET_POOLS:
        if p in s:
            return p
    return None


def extract_dividends(driver):
    """v3.1 dividend extractor — proper rowspan state machine.

    HKJC structure:
      Header row:        3 cells [pool_name, combo, dividend]
      Continuation row:  2 cells [combo, dividend]  (pool consumed by rowspan)
      Non-target pool:   3 cells with unrecognized pool name → reset state
    """
    out = []
    try:
        tables = driver.find_elements(By.CSS_SELECTOR, "table.table_bd")
        for table in tables:
            txt = table.text.upper()
            if not any(p in txt for p in TARGET_POOLS):
                continue

            last_seen_pool = None
            rows = table.find_elements(By.TAG_NAME, "tr")

            for tr in rows:
                cells = tr.find_elements(By.TAG_NAME, "td")
                n = len(cells)

                if n >= 3:
                    candidate = match_pool(cells[0].text)
                    if candidate is not None:
                        last_seen_pool = candidate
                        combo_idx = 1
                    else:
                        last_seen_pool = None
                        continue
                elif n == 2:
                    if last_seen_pool is None:
                        continue
                    combo_idx = 0
                else:
                    continue

                combo = cells[combo_idx].text.strip()
                raw   = cells[-1].text.strip()

                if 'REFUND' in raw.upper():
                    out.append({
                        "pool":      last_seen_pool,
                        "combo":     combo,
                        "dividend":  None,
                        "is_refund": 1,
                    })
                    continue

                m = re.search(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?', raw)
                if not m:
                    continue

                out.append({
                    "pool":      last_seen_pool,
                    "combo":     combo,
                    "dividend":  float(m.group(0).replace(",", "")),
                    "is_refund": 0,
                })
    except Exception as e:
        log.warning(f"Dividend extraction failed: {e}")
    return out


def extract_incidents(driver):
    """Capture the Racing Incident Report table, if present.

    HKJC's incident table has 4 columns:
      [Pla., Horse No., Horse, Incident]

    Returns a list of dicts, one per horse per race. Returns empty list
    if no incident table is present (older races sometimes omit it).

    We identify the incident table by checking each <table class='table_bd'>
    for a header row containing 'Incident' (case-insensitive). This is
    structurally distinct from the result table (which has 'Pla. Horse
    No. Horse Jockey...') and the dividend table (which has 'Pool Winning
    Combination Dividend').
    """
    out = []
    try:
        tables = driver.find_elements(By.CSS_SELECTOR, "table.table_bd")
        for table in tables:
            txt = table.text
            # Heuristic: must contain "Incident" as a column header
            # and NOT have jockey/dividend keywords
            first_line = txt.split('\n')[0].lower() if txt else ""
            if 'incident' not in first_line:
                continue
            if 'jockey' in first_line or 'dividend' in first_line:
                continue
            if 'pool' in first_line:
                continue

            rows = table.find_elements(By.TAG_NAME, "tr")
            for tr in rows:
                cells = tr.find_elements(By.TAG_NAME, "td")
                if len(cells) < 4:
                    continue  # header row or malformed

                placing = cells[0].text.strip()
                horse_no = cells[1].text.strip()
                horse_raw = cells[2].text.strip()
                incident_text = cells[3].text.strip()

                # Skip if it doesn't look like a horse row (header rows
                # have non-numeric placing)
                if not placing or not re.match(r'^\d+', placing):
                    continue

                horse_name, horse_id = parse_horse_id_from_text(horse_raw)

                out.append({
                    "placing":       placing,
                    "horse_no":      horse_no,
                    "horse_name":    horse_name,
                    "horse_id":      horse_id,
                    "incident_text": incident_text,
                })

            break  # found and parsed the incident table; stop scanning
    except Exception as e:
        log.warning(f"Incident extraction failed: {e}")
    return out


# ---------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------
def save_progress(dates):
    with open(PROGRESS_FILE, 'w') as f:
        f.write("\n".join(dates))


def load_progress():
    if not os.path.exists(PROGRESS_FILE):
        return []
    with open(PROGRESS_FILE, 'r') as f:
        return [ln.strip() for ln in f if ln.strip()]


def log_failure(meet, race_url, reason):
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
            incidents_csv = os.path.join(OUTPUT_DIR, f"incidents{count}.csv")

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
                same_day_links = []
                for x in same_day:
                    href = x.get_attribute("href")
                    if not href or "RaceNo=" not in href:
                        continue
                    if is_overseas_url(href):
                        continue
                    same_day_links.append(href)

                all_urls = [driver.current_url] + same_day_links

                race_rows     = []
                div_rows      = []
                meta_rows     = []
                incident_rows = []

                for url in all_urls:
                    if is_overseas_url(url):
                        continue

                    if url != driver.current_url:
                        driver.get(url)
                        time.sleep(1)

                    race_no   = race_no_from_url(driver.current_url)
                    abandoned = page_is_abandoned(driver)
                    hdr       = extract_race_headers(driver)
                    divs      = extract_dividends(driver)
                    incidents = extract_incidents(driver)

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
                        cols = r.find_elements(By.TAG_NAME, "td")
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
                            "is_refund": d["is_refund"],
                        })

                    for inc in incidents:
                        incident_rows.append({
                            "date":          meet,
                            "race_no":       race_no,
                            "race_name":     hdr["race_name"],
                            "placing":       inc["placing"],
                            "horse_no":      inc["horse_no"],
                            "horse_name":    inc["horse_name"],
                            "horse_id":      inc["horse_id"],
                            "incident_text": inc["incident_text"],
                        })

                    if not hdr["race_name"] and not abandoned:
                        log_failure(meet, driver.current_url, "empty_race_name")

                    status = "ABANDONED" if abandoned else f"{len(rows)} horses"
                    log.info(f"  R{race_no}: {hdr['race_name']} | "
                             f"{status} | {len(divs)} dividends | "
                             f"{len(incidents)} incidents")

                if race_rows:
                    pd.DataFrame(race_rows).to_csv(races_csv, index=False)
                if div_rows:
                    pd.DataFrame(div_rows).to_csv(dividends_csv, index=False)
                if meta_rows:
                    pd.DataFrame(meta_rows).to_csv(metadata_csv, index=False)
                if incident_rows:
                    pd.DataFrame(incident_rows).to_csv(incidents_csv, index=False)

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
