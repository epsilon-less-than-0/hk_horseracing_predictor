"""
HKJC v32 Database Ingest Pipeline
==================================
Phase 55.2 — Loads scraped CSV trio (races/dividends/metadata) into a clean
SQLite database with three primary tables, proper indexes, and ISO date keys.

Strategy: drop-and-recreate every run. Raw CSVs are the source of truth.

Inputs:
  data/raw_csvs/races{N}.csv      (positional columns, no header row)
  data/raw_csvs/dividends{N}.csv  (headed, may be empty for Conghua meetings)
  data/raw_csvs/metadata{N}.csv   (headed)

Output:
  data/hk_racing.db

Tables produced:
  race_results       — one row per (race, horse), only finishing entries
  exotic_dividends   — one row per (race, pool, combo)
  race_metadata      — one row per race

Protocol decisions enforced (see project log Phase 55.1):
  D1. Overseas simulcasts filtered (not present in scraped CSVs anyway)
  D2. Conghua training meets marked is_bettable=0 (no dividends)
  D3. Abandoned races marked is_refund=1, dividend=NULL
  D4. race_id is canonical key: "{YYYY-MM-DD}_R{race_no}"
  D5. horse_id parsed from "(CODE)" pattern

Run from project root: python3 data_pipeline/ingest_v32.py
"""

import os
import re
import sys
import glob
import logging
import sqlite3
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse, parse_qs

import pandas as pd
import numpy as np

# ---------------------------------------------------------------------
# Path resolution — works from any cwd
# ---------------------------------------------------------------------
_SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
RAW_CSV_DIR   = os.path.join(_PROJECT_ROOT, "data", "raw_csvs")
DB_PATH       = os.path.join(_PROJECT_ROOT, "data", "hk_racing.db")
LOG_PATH      = os.path.join(_PROJECT_ROOT, "data", "ingest_log.txt")

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()]
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Column mapping for races{N}.csv (positional, no header row)
# ---------------------------------------------------------------------
RACE_COLS = [
    'race_name', 'going', 'course',         # cols 0-2: race-level (denormalized)
    'finish_position',                       # col 3
    'horse_no',                              # col 4
    'horse_raw',                             # col 5: "NAME (CODE)"
    'jockey',                                # col 6
    'trainer',                               # col 7
    'act_wt',                                # col 8
    'horse_wt',                              # col 9
    'draw',                                  # col 10
    'lbw',                                   # col 11
    'running_pos',                           # col 12
    'finish_time',                           # col 13
    'win_odds',                              # col 14
]


# =====================================================================
# PHASE 1 — LOAD CSVs
# =====================================================================
def load_all_csvs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load and concatenate all races/dividends/metadata CSVs."""
    log.info("=" * 70)
    log.info("PHASE 1: Loading CSVs")
    log.info("=" * 70)

    race_files = sorted(glob.glob(os.path.join(RAW_CSV_DIR, "races*.csv")))
    div_files  = sorted(glob.glob(os.path.join(RAW_CSV_DIR, "dividends*.csv")))
    meta_files = sorted(glob.glob(os.path.join(RAW_CSV_DIR, "metadata*.csv")))

    log.info(f"Found {len(race_files)} race files, {len(div_files)} dividend "
             f"files, {len(meta_files)} metadata files")

    # --- Races: no header, skip the integer-index row pandas wrote ---
    races_dfs = []
    for f in race_files:
        try:
            df = pd.read_csv(f, header=None, skiprows=1, names=RACE_COLS,
                             dtype=str, keep_default_na=False)
            df['_source_file'] = os.path.basename(f)
            races_dfs.append(df)
        except Exception as e:
            log.warning(f"  Skipping {f}: {e}")
    races = pd.concat(races_dfs, ignore_index=True)
    log.info(f"  Loaded {len(races):,} race-horse rows")

    # --- Dividends: headed, some files may be empty (Conghua meetings) ---
    divs_dfs = []
    for f in div_files:
        try:
            df = pd.read_csv(f, dtype=str, keep_default_na=False)
            if len(df) > 0:
                divs_dfs.append(df)
        except pd.errors.EmptyDataError:
            pass  # Conghua / no-dividend meetings — expected
        except Exception as e:
            log.warning(f"  Skipping {f}: {e}")
    divs = pd.concat(divs_dfs, ignore_index=True)
    log.info(f"  Loaded {len(divs):,} dividend rows")

    # --- Metadata: headed ---
    meta_dfs = []
    for f in meta_files:
        try:
            df = pd.read_csv(f, dtype=str, keep_default_na=False)
            meta_dfs.append(df)
        except Exception as e:
            log.warning(f"  Skipping {f}: {e}")
    meta = pd.concat(meta_dfs, ignore_index=True)
    log.info(f"  Loaded {len(meta):,} metadata rows")

    return races, divs, meta


# =====================================================================
# PHASE 2 — CLEAN AND ENRICH
# =====================================================================
def ddmmyyyy_to_iso(s: str) -> Optional[str]:
    """'15/04/2026' -> '2026-04-15'. None on failure."""
    try:
        return datetime.strptime(s.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except Exception:
        return None


def parse_horse_id(raw: str) -> tuple[str, str]:
    """'FLYING AMANI (K152)' -> ('FLYING AMANI', 'K152'). Falls back to raw
    name if no code present."""
    m = re.match(r'^\s*(.+?)\s*\(([A-Z0-9]{3,5})\)\s*$', str(raw))
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return str(raw).strip(), str(raw).strip().upper()[:5]


def parse_venue_from_url(url: str) -> str:
    """Pull Racecourse parameter from results URL. Returns HV/ST/CH/UNK."""
    if not url or pd.isna(url):
        return "UNK"
    try:
        q = parse_qs(urlparse(url).query.lower())
        # Query keys are lowercased; HKJC's actual keys vary case (Racecourse vs racecourse)
        for key in ('racecourse',):
            if key in q and q[key] and q[key][0]:
                return q[key][0].upper()
    except Exception:
        pass
    return "UNK"


def to_int(s, default=None):
    """Coerce to int, return default on failure."""
    try:
        if s is None or s == '' or s == '---':
            return default
        return int(float(str(s).strip()))
    except (ValueError, TypeError):
        return default


def to_float(s, default=None):
    """Coerce to float, return default on failure."""
    try:
        if s is None or s == '' or s == '---':
            return default
        return float(str(s).strip())
    except (ValueError, TypeError):
        return default


def clean_metadata(meta: pd.DataFrame) -> pd.DataFrame:
    """Build the race_metadata table with venue, is_bettable, ISO date, race_id."""
    log.info("-" * 70)
    log.info("Cleaning metadata")

    df = meta.copy()
    df['date_iso'] = df['date'].apply(ddmmyyyy_to_iso)
    df['race_no']  = df['race_no'].apply(to_int)
    df = df.dropna(subset=['date_iso', 'race_no']).copy()
    df['race_no'] = df['race_no'].astype(int)

    df['race_id']  = df['date_iso'] + '_R' + df['race_no'].astype(str)
    df['distance'] = df['distance'].apply(lambda x: to_int(x))
    df['prize']    = df['prize'].apply(lambda x: to_int(x))
    df['venue']    = df['url'].apply(parse_venue_from_url)

    # is_bettable: default 1, set to 0 for Conghua meetings (CH venue, no dividends).
    # The dividend check happens after we have the dividends frame, so default here
    # and patch from outside. For now, set tentative based on venue alone.
    df['is_bettable'] = 1  # patched in main()

    # Deduplicate (in case the same race appears in multiple metadata files)
    df = df.drop_duplicates(subset='race_id', keep='last').reset_index(drop=True)

    log.info(f"  Cleaned metadata: {len(df):,} unique races")
    log.info(f"  Venue distribution: {df['venue'].value_counts().to_dict()}")

    return df[[
        'date', 'date_iso', 'race_no', 'race_id',
        'race_name', 'going', 'course', 'distance',
        'race_class', 'prize', 'venue', 'is_bettable', 'url'
    ]]


def clean_dividends(divs: pd.DataFrame) -> pd.DataFrame:
    """Build the exotic_dividends table. Detect REFUND, set is_refund flag."""
    log.info("-" * 70)
    log.info("Cleaning dividends")

    df = divs.copy()
    df['date_iso'] = df['date'].apply(ddmmyyyy_to_iso)
    df['race_no']  = df['race_no'].apply(to_int)
    df = df.dropna(subset=['date_iso', 'race_no']).copy()
    df['race_no'] = df['race_no'].astype(int)
    df['race_id'] = df['date_iso'] + '_R' + df['race_no'].astype(str)

    # Refund detection. The raw 'dividend' column was scraped as a numeric
    # string. The v2.1 scraper's regex actually skipped REFUND cells entirely
    # (regex \d{1,3}... didn't match), so REFUND rows are not in the CSVs at all.
    # We still defensively check for any string contamination.
    div_str = df['dividend'].astype(str).str.upper()
    df['is_refund'] = div_str.str.contains('REFUND', na=False).astype(int)
    df['dividend'] = df['dividend'].apply(
        lambda x: to_float(x) if 'REFUND' not in str(x).upper() else None
    )

    # Deduplicate on (race_id, pool, combo)
    df = df.drop_duplicates(subset=['race_id', 'pool', 'combo'],
                            keep='last').reset_index(drop=True)

    log.info(f"  Cleaned dividends: {len(df):,} rows")
    log.info(f"  Refund rows: {df['is_refund'].sum():,}")
    log.info(f"  Pool distribution:\n{df['pool'].value_counts().to_string()}")

    return df[[
        'date', 'date_iso', 'race_no', 'race_id',
        'pool', 'combo', 'dividend', 'is_refund'
    ]]


def clean_race_results(races: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    """Build the race_results table.

    OPTION A enforcement: only insert rows where finish_position is populated.
    This excludes abandoned races (no result table) entirely from race_results.

    The races CSVs lack a date column — we infer it from the source file
    by joining on _source_file → meeting → date via the metadata CSVs."""
    log.info("-" * 70)
    log.info("Cleaning race_results")

    df = races.copy()

    # Build a meeting-number -> date map from metadata file naming.
    # meta CSVs are headed; races CSVs share their meeting number.
    src_to_date = {}
    for src in df['_source_file'].unique():
        # src looks like "races123.csv" → meeting 123
        m = re.match(r'races(\d+)\.csv$', src)
        if not m:
            continue
        n = m.group(1)
        meta_path = os.path.join(RAW_CSV_DIR, f"metadata{n}.csv")
        if not os.path.exists(meta_path):
            continue
        try:
            meta_n = pd.read_csv(meta_path, dtype=str, keep_default_na=False)
            if len(meta_n):
                src_to_date[src] = meta_n['date'].iloc[0]
        except Exception:
            continue

    df['date']     = df['_source_file'].map(src_to_date)
    df['date_iso'] = df['date'].apply(ddmmyyyy_to_iso)

    # Parse race_no from race_name "RACE 7 (NNN)"
    df['race_no'] = df['race_name'].str.extract(r'RACE\s+(\d+)', expand=False).apply(to_int)

    # Drop rows we can't key
    df = df.dropna(subset=['date_iso', 'race_no']).copy()
    df['race_no'] = df['race_no'].astype(int)
    df['race_id'] = df['date_iso'] + '_R' + df['race_no'].astype(str)

    # Parse horse_id and horse_name
    parsed = df['horse_raw'].apply(parse_horse_id)
    df['horse_name'] = parsed.apply(lambda t: t[0])
    df['horse_id']   = parsed.apply(lambda t: t[1])

    # Numeric coercions
    df['finish_position'] = df['finish_position'].apply(to_int)
    df['horse_no']        = df['horse_no'].apply(to_int)
    df['act_wt']          = df['act_wt'].apply(to_float)
    df['horse_wt']        = df['horse_wt'].apply(to_int)
    df['draw']            = df['draw'].apply(to_int)
    df['win_odds']        = df['win_odds'].apply(to_float)
    df['distance']        = None  # backfill from metadata in a moment

    # OPTION A: drop rows with no finish_position (abandoned/scratched mid-race).
    pre = len(df)
    df = df.dropna(subset=['finish_position']).copy()
    df['finish_position'] = df['finish_position'].astype(int)
    log.info(f"  Dropped {pre - len(df):,} rows with no finish_position (Option A)")

    # Drop rows with no horse_no
    pre = len(df)
    df = df.dropna(subset=['horse_no']).copy()
    df['horse_no'] = df['horse_no'].astype(int)
    if pre - len(df) > 0:
        log.info(f"  Dropped {pre - len(df):,} rows with no horse_no")

    # Backfill race-level distance from metadata
    meta_dist = meta.set_index('race_id')['distance'].to_dict()
    df['distance'] = df['race_id'].map(meta_dist)

    # Deduplicate on (race_id, horse_id)
    pre = len(df)
    df = df.drop_duplicates(subset=['race_id', 'horse_id'],
                            keep='last').reset_index(drop=True)
    if pre - len(df) > 0:
        log.info(f"  Dropped {pre - len(df):,} duplicate (race, horse) rows")

    log.info(f"  Cleaned race_results: {len(df):,} rows")

    return df[[
        'date', 'date_iso', 'race_no', 'race_id',
        'race_name', 'going', 'course', 'distance',
        'finish_position', 'horse_no', 'horse_id', 'horse_name',
        'jockey', 'trainer', 'act_wt', 'horse_wt', 'draw',
        'lbw', 'running_pos', 'finish_time', 'win_odds'
    ]]


def apply_bettable_flag(meta: pd.DataFrame, divs: pd.DataFrame) -> pd.DataFrame:
    """A meeting is non-bettable if venue=CH AND it has zero dividend rows
    across all its races. (Conghua training meets.)

    A race is non-bettable if it's part of a non-bettable meeting OR
    all its dividend rows are refunds.
    """
    log.info("-" * 70)
    log.info("Applying is_bettable flag to metadata")

    # Per-race dividend counts (real, non-refund only)
    real_divs = divs[divs['is_refund'] == 0]
    race_div_counts = real_divs.groupby('race_id').size().to_dict()

    # Per-date dividend counts (entire meeting)
    meta = meta.copy()
    meta['real_divs_for_race'] = meta['race_id'].map(race_div_counts).fillna(0).astype(int)

    meeting_div_counts = meta.groupby('date_iso')['real_divs_for_race'].sum().to_dict()
    meta['real_divs_for_meeting'] = meta['date_iso'].map(meeting_div_counts).astype(int)

    # is_bettable=0 if either:
    #   - The meeting has zero real dividends (Conghua training)
    #   - This race specifically has zero real dividends (abandoned)
    meta['is_bettable'] = (
        (meta['real_divs_for_race'] > 0) & (meta['real_divs_for_meeting'] > 0)
    ).astype(int)

    log.info(f"  Bettable races:     {meta['is_bettable'].sum():,}")
    log.info(f"  Non-bettable races: {(meta['is_bettable']==0).sum():,}")
    log.info(f"  Non-bettable by venue: \n"
             f"{meta[meta['is_bettable']==0]['venue'].value_counts().to_string()}")

    return meta.drop(columns=['real_divs_for_race', 'real_divs_for_meeting'])


# =====================================================================
# PHASE 3 — WRITE TO SQLITE
# =====================================================================
SCHEMA_SQL = """
DROP TABLE IF EXISTS race_results;
DROP TABLE IF EXISTS exotic_dividends;
DROP TABLE IF EXISTS race_metadata;

CREATE TABLE race_results (
    date            TEXT NOT NULL,
    date_iso        TEXT NOT NULL,
    race_no         INTEGER NOT NULL,
    race_id         TEXT NOT NULL,
    race_name       TEXT,
    going           TEXT,
    course          TEXT,
    distance        INTEGER,
    finish_position INTEGER NOT NULL,
    horse_no        INTEGER NOT NULL,
    horse_id        TEXT NOT NULL,
    horse_name      TEXT,
    jockey          TEXT,
    trainer         TEXT,
    act_wt          REAL,
    horse_wt        INTEGER,
    draw            INTEGER,
    lbw             TEXT,
    running_pos     TEXT,
    finish_time     TEXT,
    win_odds        REAL,
    PRIMARY KEY (race_id, horse_id)
);

CREATE TABLE exotic_dividends (
    date            TEXT NOT NULL,
    date_iso        TEXT NOT NULL,
    race_no         INTEGER NOT NULL,
    race_id         TEXT NOT NULL,
    pool            TEXT NOT NULL,
    combo           TEXT NOT NULL,
    dividend        REAL,
    is_refund       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (race_id, pool, combo)
);

CREATE TABLE race_metadata (
    date            TEXT NOT NULL,
    date_iso        TEXT NOT NULL,
    race_no         INTEGER NOT NULL,
    race_id         TEXT PRIMARY KEY,
    race_name       TEXT,
    going           TEXT,
    course          TEXT,
    distance        INTEGER,
    race_class      TEXT,
    prize           INTEGER,
    venue           TEXT,
    is_bettable     INTEGER NOT NULL DEFAULT 1,
    url             TEXT
);
"""

INDEX_SQL = """
CREATE INDEX idx_results_date  ON race_results(date_iso);
CREATE INDEX idx_results_horse ON race_results(horse_id, date_iso);
CREATE INDEX idx_results_race  ON race_results(race_id);
CREATE INDEX idx_results_jock  ON race_results(jockey, date_iso);
CREATE INDEX idx_results_train ON race_results(trainer, date_iso);

CREATE INDEX idx_div_race  ON exotic_dividends(race_id);
CREATE INDEX idx_div_pool  ON exotic_dividends(pool, date_iso);
CREATE INDEX idx_div_date  ON exotic_dividends(date_iso);

CREATE INDEX idx_meta_date   ON race_metadata(date_iso);
CREATE INDEX idx_meta_venue  ON race_metadata(venue);
CREATE INDEX idx_meta_bet    ON race_metadata(is_bettable, date_iso);
"""


def write_to_db(results: pd.DataFrame,
                divs: pd.DataFrame,
                meta: pd.DataFrame) -> None:
    log.info("=" * 70)
    log.info(f"PHASE 3: Writing to SQLite at {DB_PATH}")
    log.info("=" * 70)

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        log.info("  Deleted existing database file")

    conn = sqlite3.connect(DB_PATH)
    try:
        # Schema
        conn.executescript(SCHEMA_SQL)
        conn.commit()
        log.info("  Schema created")

        # Inserts (use 'append' since we just created empty tables)
        meta.to_sql('race_metadata',    conn, if_exists='append',
                    index=False, method='multi', chunksize=500)
        log.info(f"  Inserted {len(meta):,} metadata rows")

        divs.to_sql('exotic_dividends', conn, if_exists='append',
                    index=False, method='multi', chunksize=500)
        log.info(f"  Inserted {len(divs):,} dividend rows")

        results.to_sql('race_results', conn, if_exists='append',
                       index=False, method='multi', chunksize=500)
        log.info(f"  Inserted {len(results):,} race_results rows")

        # Indexes (faster to create after bulk insert)
        conn.executescript(INDEX_SQL)
        conn.commit()
        log.info("  Indexes created")

        # VACUUM to optimize storage
        conn.execute("VACUUM;")
        log.info("  VACUUM complete")

    finally:
        conn.close()


# =====================================================================
# SANITY CHECKS
# =====================================================================
def sanity_checks() -> None:
    log.info("=" * 70)
    log.info("SANITY CHECKS")
    log.info("=" * 70)

    conn = sqlite3.connect(DB_PATH)
    try:
        # 1. Total counts
        for tbl in ('race_results', 'exotic_dividends', 'race_metadata'):
            n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            log.info(f"  {tbl}: {n:,} rows")

        # 2. Bettable vs non-bettable
        q = """
        SELECT is_bettable, COUNT(*) FROM race_metadata GROUP BY is_bettable
        """
        log.info("\n  Bettable races:")
        for row in conn.execute(q):
            log.info(f"    is_bettable={row[0]}: {row[1]:,}")

        # 3. Refund rows
        n_refund = conn.execute(
            "SELECT COUNT(*) FROM exotic_dividends WHERE is_refund=1"
        ).fetchone()[0]
        log.info(f"\n  Refund dividend rows: {n_refund:,}")

        # 4. Race count by HKJC season (Sep-Aug)
        log.info("\n  Bettable races by HKJC season (Sep YYYY → Aug YYYY+1):")
        q = """
        SELECT
          CASE
            WHEN CAST(substr(date_iso,6,2) AS INTEGER) >= 9
              THEN substr(date_iso,1,4) || '/' || (CAST(substr(date_iso,1,4) AS INTEGER)+1)
            ELSE (CAST(substr(date_iso,1,4) AS INTEGER)-1) || '/' || substr(date_iso,1,4)
          END AS season,
          COUNT(*) AS bettable_races
        FROM race_metadata
        WHERE is_bettable = 1
        GROUP BY season
        ORDER BY season;
        """
        for row in conn.execute(q):
            log.info(f"    {row[0]}: {row[1]:,} races")

        # 5. Dividend coverage by pool
        log.info("\n  Real (non-refund) dividend rows by pool:")
        q = """
        SELECT pool, COUNT(*) FROM exotic_dividends
        WHERE is_refund = 0
        GROUP BY pool ORDER BY pool;
        """
        for row in conn.execute(q):
            log.info(f"    {row[0]:<16}: {row[1]:,}")

        # 6. Orphan detection
        log.info("\n  Orphan detection:")
        n = conn.execute("""
            SELECT COUNT(*) FROM race_results r
            LEFT JOIN race_metadata m ON r.race_id = m.race_id
            WHERE m.race_id IS NULL
        """).fetchone()[0]
        log.info(f"    race_results without matching metadata: {n}")

        n = conn.execute("""
            SELECT COUNT(DISTINCT r.race_id) FROM race_results r
            LEFT JOIN exotic_dividends d ON r.race_id = d.race_id
            WHERE d.race_id IS NULL
        """).fetchone()[0]
        log.info(f"    races_results races with NO dividends: {n}")

        # 7. Date range
        row = conn.execute(
            "SELECT MIN(date_iso), MAX(date_iso) FROM race_metadata"
        ).fetchone()
        log.info(f"\n  Date range: {row[0]} → {row[1]}")

        # 8. Sample row from each table for smoke test
        log.info("\n  Sample race_results row:")
        cols = [d[0] for d in conn.execute("SELECT * FROM race_results LIMIT 1").description]
        row = conn.execute("SELECT * FROM race_results LIMIT 1").fetchone()
        for c, v in zip(cols, row):
            log.info(f"    {c}: {v}")

    finally:
        conn.close()


# =====================================================================
# MAIN
# =====================================================================
def main():
    start = datetime.now()
    log.info("#" * 70)
    log.info(f"# v32 INGEST STARTING — {start}")
    log.info("#" * 70)

    races_raw, divs_raw, meta_raw = load_all_csvs()

    meta = clean_metadata(meta_raw)
    divs = clean_dividends(divs_raw)
    meta = apply_bettable_flag(meta, divs)
    results = clean_race_results(races_raw, meta)

    write_to_db(results, divs, meta)

    sanity_checks()

    elapsed = (datetime.now() - start).total_seconds()
    log.info("#" * 70)
    log.info(f"# INGEST COMPLETE — {elapsed:.1f} seconds")
    log.info("#" * 70)


if __name__ == "__main__":
    main()
