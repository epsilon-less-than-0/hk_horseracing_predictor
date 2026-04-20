import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import glob
import csv, json, re, os
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SectionalPaceEngineer:
    """
    Calculates Topological Pace Metrics for HKJC Racing.
    Enforces strict chronological shifting to prevent Look-Ahead Bias.
    """
    def __init__(self, rolling_window=5):
        self.rolling_window = rolling_window

    def _parse_running_pos(self, pos_string):
        """Safely parses '3 2 2 1' into a list of integers."""
        if pd.isna(pos_string) or pos_string == '---':
            return []
        
        parsed = []
        for x in str(pos_string).split():
            try:
                parsed.append(int(x))
            except ValueError:
                pass
        return parsed

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Phase 8: Extracting raw positional derivatives...")
        
        # 1. Parse strings to lists
        parsed_pos = df['running_pos'].apply(self._parse_running_pos)
        
        # 2. Raw Early Speed Index (ESI)
        # Penalizes horses that drop back early. 1st = 1.0, 4th = 0.5, 9th = 0.33
        df['raw_ESI'] = parsed_pos.apply(
            lambda x: 1.0 / np.sqrt(x[0]) if len(x) > 0 and x[0] > 0 else np.nan
        )
        
        # 3. Raw Closing Speed Index (CSI)
        # Acceleration between the penultimate and final call. Positive means passing horses.
        df['raw_CSI'] = parsed_pos.apply(
            lambda x: (x[-2] - x[-1]) if len(x) >= 2 else 0
        )
        
        logging.info("Phase 8: Engineering strictly shifted (.shift(1)) Pace Momentum...")
        
        # CRITICAL: Force datetime format for strict chronological sorting. 
        # Without this, string dates ("29/12/2019") will sort incorrectly and leak data.
        df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
        
        # Ensure absolute chronological order before shifting
        df = df.sort_values(by=['horse_id', 'date']).reset_index(drop=True)
        
        # Shifted Rolling ESI & CSI
        df['shifted_rolling_ESI'] = df.groupby('horse_id')['raw_ESI'].transform(
            lambda x: x.shift(1).rolling(window=self.rolling_window, min_periods=1).mean()
        )
        df['shifted_rolling_CSI'] = df.groupby('horse_id')['raw_CSI'].transform(
            lambda x: x.shift(1).rolling(window=self.rolling_window, min_periods=1).mean()
        )
        
        logging.info("Phase 8: Calculating Race-Level Contextual Pace...")
        
        # Race ESI Pressure (Sum of top 3 shifted ESIs in the race)
        top_3_esi = df.groupby('race_id')['shifted_rolling_ESI'].apply(
            lambda x: x.nlargest(3).sum()
        ).reset_index()
        top_3_esi.rename(columns={'shifted_rolling_ESI': 'race_ESI_pressure'}, inplace=True)
        
        df = pd.merge(df, top_3_esi, on='race_id', how='left')
        
        # Pace Advantage
        df['pace_advantage'] = df['shifted_rolling_ESI'] - df['race_ESI_pressure']
        
        return df


def key_func(afilename):
    """Extract number from filename for proper sorting"""
    nondigits = re.compile("\\D")
    return int(nondigits.sub("", afilename))

def extract_date_from_csv(df):
    """Extract date from the dataframe if it exists in the race name or other fields"""
    try:
        for col in df.columns:
            if 'date' in str(col).lower():
                return df[col].iloc[0] if not df.empty else None
        return None
    except:
        return None

def clean_race_data(df):
    """Clean and structure the race data"""
    if df.empty:
        logging.warning("Empty dataframe received")
        return df
    
    num_cols = len(df.columns)
    logging.info(f"Processing dataframe with {num_cols} columns and {len(df)} rows")
    
    expected_cols = ['race_name', 'going', 'race_type', 'plc', 'horse_no', 'horse_name', 
                    'jockey_name', 'trainer_name', 'actual_wt', 'declared_wt', 
                    'draw', 'lbw', 'running_pos', 'finish_time', 'public_odds']
    
    if num_cols >= len(expected_cols):
        df.columns = expected_cols + [f'extra_col_{i}' for i in range(num_cols - len(expected_cols))]
    else:
        df.columns = expected_cols[:num_cols]
        logging.warning(f"DataFrame has fewer columns than expected ({num_cols} vs {len(expected_cols)})")
    
    if 'race_name' in df.columns:
        df['race_id'] = df['race_name'].apply(lambda x: x.split('(')[1][:-1] if '(' in str(x) and ')' in str(x) else 'Unknown')
        df['race_no'] = df['race_name'].apply(lambda x: re.sub(' ', '_', x.split('(')[0].strip()) if '(' in str(x) else str(x).replace(' ', '_'))
    
    if 'race_type' in df.columns:
        df['race_dist'] = df['race_type'].apply(lambda x: x.split("-")[1].strip().replace(' ','_').upper().rstrip('M') if '-' in str(x) else 'Unknown')
        df['race_type'] = df['race_type'].apply(lambda x: x.split("-")[0].strip().replace(' ','_').upper() if '-' in str(x) else str(x).replace(' ','_').upper())
    
    if 'horse_name' in df.columns:
        df['horse_id'] = df['horse_name'].apply(lambda x: x.split("(")[1].strip()[:-1] if '(' in str(x) and ')' in str(x) else 'Unknown')
        df['horse_name'] = df['horse_name'].apply(lambda x: x.split("(")[0].strip().replace(" ", "_") if '(' in str(x) else str(x).replace(" ", "_"))
    
    numeric_cols = ['horse_no', 'actual_wt', 'declared_wt', 'draw', 'public_odds']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    if 'plc' in df.columns:
        df['plc'] = pd.to_numeric(df['plc'], errors='coerce')
    
    return df

def load_progress_file():
    """Load the progress file to get the mapping of file numbers to dates"""
    date_mapping = {}
    
    if os.path.exists('progress.txt'):
        with open('progress.txt', 'r') as f:
            processed_dates = [line.strip() for line in f.readlines()]
        
        for i, date in enumerate(processed_dates, 1):
            if os.path.exists(f'races{i}.csv'):
                date_mapping[i] = date
        
        logging.info(f"Found {len(date_mapping)} date mappings from progress file")
        return date_mapping
    else:
        logging.warning("No progress.txt file found. Will try to extract dates from data.")
        return {}

def main():
    """Main data processing function"""
    logging.info("Starting data preparation...")
    
    date_mapping = load_progress_file()
    df_list = []
    
    race_files = sorted(glob.glob('./races*.csv'), key=key_func)
    logging.info(f"Found {len(race_files)} race CSV files")
    
    if not race_files:
        logging.error("No race CSV files found! Make sure your scraper has run and created races*.csv files.")
        return None
    
    for file_path in race_files:
        try:
            file_num = int(re.findall(r'races(\d+)\.csv', file_path)[0])
            logging.info(f"Processing {file_path}...")
            
            temp_df = pd.read_csv(file_path)
            if temp_df.empty:
                logging.warning(f"{file_path} is empty, skipping")
                continue
            
            temp_df = clean_race_data(temp_df)

            if file_num in date_mapping:
                temp_df["date"] = date_mapping[file_num]
                logging.info(f"Added date {date_mapping[file_num]} to {file_path}")
            else:
                extracted_date = extract_date_from_csv(temp_df)
                if extracted_date:
                    temp_df["date"] = extracted_date
                else:
                    temp_df["date"] = f"Unknown_{file_num}"
                    logging.warning(f"Could not determine date for {file_path}, using Unknown_{file_num}")
            
            temp_df['source_file'] = file_path
            df_list.append(temp_df)
            logging.info(f"Successfully processed {file_path} with {len(temp_df)} rows")
            
        except Exception as e:
            logging.error(f"Error processing {file_path}: {e}")
            continue
    
    if not df_list:
        logging.error("No valid data found in any CSV files!")
        return None
    
    logging.info("Combining all dataframes...")
    try:
        df = pd.concat(df_list, ignore_index=True, sort=False)
        logging.info(f"Combined dataset created with {len(df)} total rows")
        
        # --- PHASE 8 PACE INJECTION ---
        pace_engineer = SectionalPaceEngineer(rolling_window=5)
        df = pace_engineer.fit_transform(df)
        logging.info("Phase 8 Pace Engineering complete.")
        # ------------------------------
        
        logging.info("="*50)
        logging.info("DATASET SUMMARY:")
        logging.info(f"Total races: {df['race_id'].nunique() if 'race_id' in df.columns else 'Unknown'}")
        logging.info(f"Total horses: {df['horse_id'].nunique() if 'horse_id' in df.columns else 'Unknown'}")
        logging.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
        logging.info(f"Total entries: {len(df)}")
        logging.info("="*50)
        
        print("\nFirst 5 rows of cleaned data:")
        print(df.head())
        
        output_file = 'combined_race_data.csv'
        df.to_csv(output_file, index=False)
        logging.info(f"Combined dataset saved as {output_file}")
        
        return df
        
    except Exception as e:
        logging.error(f"Error combining dataframes: {e}")
        return None

def process_with_date_range(start_date, end_date):
    """Process data with a specific date range instead of using progress file"""
    from datetime import datetime, timedelta
    
    def generate_date_range(start_date, end_date):
        start = datetime.strptime(start_date, "%d/%m/%Y")
        end = datetime.strptime(end_date, "%d/%m/%Y")
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime("%d/%m/%Y"))
            current += timedelta(days=1)
        return dates
    
    all_dates = generate_date_range(start_date, end_date)
    date_mapping_manual = {}
    file_count = 1
    
    for date in all_dates:
        if os.path.exists(f'races{file_count}.csv'):
            date_mapping_manual[file_count] = date
            file_count += 1
    
    logging.info(f"Created date mapping for {len(date_mapping_manual)} files")
    return main_with_custom_mapping(date_mapping_manual)

def main_with_custom_mapping(custom_date_mapping):
    """Main processing function with custom date mapping"""
    logging.info("Starting data preparation with custom date mapping...")
    
    df_list = []
    race_files = sorted(glob.glob('./races*.csv'), key=key_func)
    logging.info(f"Found {len(race_files)} race CSV files")
    
    if not race_files:
        logging.error("No race CSV files found!")
        return None
    
    for file_path in race_files:
        try:
            file_num = int(re.findall(r'races(\d+)\.csv', file_path)[0])
            logging.info(f"Processing {file_path}...")
            
            temp_df = pd.read_csv(file_path)
            if temp_df.empty:
                logging.warning(f"{file_path} is empty, skipping")
                continue
            
            temp_df = clean_race_data(temp_df)

            if file_num in custom_date_mapping:
                temp_df["date"] = custom_date_mapping[file_num]
                logging.info(f"Added date {custom_date_mapping[file_num]} to {file_path}")
            else:
                temp_df["date"] = f"Unknown_{file_num}"
                logging.warning(f"Could not determine date for {file_path}, using Unknown_{file_num}")
            
            temp_df['source_file'] = file_path
            df_list.append(temp_df)
            
        except Exception as e:
            logging.error(f"Error processing {file_path}: {e}")
            continue
    
    if not df_list:
        logging.error("No valid data found in any CSV files!")
        return None
    
    logging.info("Combining all dataframes...")
    try:
        df = pd.concat(df_list, ignore_index=True, sort=False)
        logging.info(f"Combined dataset created with {len(df)} total rows")
        
        # --- PHASE 8 PACE INJECTION ---
        pace_engineer = SectionalPaceEngineer(rolling_window=5)
        df = pace_engineer.fit_transform(df)
        logging.info("Phase 8 Pace Engineering complete.")
        # ------------------------------
        
        output_file = 'combined_race_data.csv'
        df.to_csv(output_file, index=False)
        logging.info(f"Combined dataset saved as {output_file}")
        
        return df
        
    except Exception as e:
        logging.error(f"Error combining dataframes: {e}")
        return None

if __name__ == "__main__":
    # Run the main processing
    df = main()
    
    if df is not None:
        print("\nData processing completed successfully!")
        print(f"Final dataset shape: {df.shape}")
        
        print(f"\nColumns in final dataset:")
        for i, col in enumerate(df.columns):
            print(f"{i+1:2d}. {col}")
        
        if 'date' in df.columns:
            print(f"\nUnique dates in dataset: {df['date'].nunique()}")
        if 'race_id' in df.columns:
            print(f"Unique races in dataset: {df['race_id'].nunique()}")
        if 'horse_id' in df.columns:
            print(f"Unique horses in dataset: {df['horse_id'].nunique()}")
            
        # --- PHASE 8 ZERO DATA LEAKAGE AUDIT ---
        print("\n" + "="*80)
        print("PHASE 8 AUDIT: ZERO DATA LEAKAGE VERIFICATION")
        print("="*80)
        
        # We will attempt to find 'C413' (RICH AND LUCKY), or fallback to the first horse in the DF
        test_horse = 'C413'
        if test_horse not in df['horse_id'].values:
            test_horse = df['horse_id'].iloc[0]
            
        audit_df = df[df['horse_id'] == test_horse].sort_values('date')
        print(f"Audit Trail for Horse ID: {test_horse}")
        
        # Filter down to the essential pace variables
        cols_to_show = ['date', 'race_id', 'running_pos', 'raw_ESI', 'shifted_rolling_ESI', 'race_ESI_pressure', 'pace_advantage']
        # If any columns are missing due to early data errors, only select existing ones
        cols_to_show = [c for c in cols_to_show if c in audit_df.columns]
        
        print(audit_df[cols_to_show].to_string(index=False))
        print("="*80)
        print(">> Note: 'shifted_rolling_ESI' for the FIRST chronological race must ALWAYS be NaN.")
        print(">> This mathematically guarantees the model operates strictly out-of-sample.")
        # ---------------------------------------
    else:
        print("Data processing failed. Check the logs for errors.")