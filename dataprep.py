import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import glob
import csv, json, re, os
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def key_func(afilename):
    """Extract number from filename for proper sorting"""
    nondigits = re.compile("\\D")
    return int(nondigits.sub("", afilename))

def extract_date_from_csv(df):
    """Extract date from the dataframe if it exists in the race name or other fields"""
    try:
        # Look for date patterns in the data itself
        # This is a fallback if date isn't stored separately
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
    
    # Get column count to handle variable column structures
    num_cols = len(df.columns)
    logging.info(f"Processing dataframe with {num_cols} columns and {len(df)} rows")
    
    # Expected columns based on your scraper:
    # [race_name, race_going, race_type, place, horse_no, horse_name, jockey, trainer, 
    #  actual_wt, declared_wt, draw, lbw, running_pos, finish_time, win_odds, ...]
    
    expected_cols = ['race_name', 'going', 'race_type', 'plc', 'horse_no', 'horse_name', 
                    'jockey_name', 'trainer_name', 'actual_wt', 'declared_wt', 
                    'draw', 'lbw', 'running_pos', 'finish_time', 'public_odds']
    
    # Assign column names based on expected structure
    if num_cols >= len(expected_cols):
        df.columns = expected_cols + [f'extra_col_{i}' for i in range(num_cols - len(expected_cols))]
    else:
        df.columns = expected_cols[:num_cols]
        logging.warning(f"DataFrame has fewer columns than expected ({num_cols} vs {len(expected_cols)})")
    
    # Clean race information
    if 'race_name' in df.columns:
        # Extract race ID from race name (e.g., "RACE 1 (284)" -> race_id: 284, race_no: RACE_1)
        df['race_id'] = df['race_name'].apply(lambda x: x.split('(')[1][:-1] if '(' in str(x) and ')' in str(x) else 'Unknown')
        df['race_no'] = df['race_name'].apply(lambda x: re.sub(' ', '_', x.split('(')[0].strip()) if '(' in str(x) else str(x).replace(' ', '_'))
    
    # Clean race type and extract distance
    if 'race_type' in df.columns:
        df['race_dist'] = df['race_type'].apply(lambda x: x.split("-")[1].strip().replace(' ','_').upper().rstrip('M') if '-' in str(x) else 'Unknown')
        df['race_type'] = df['race_type'].apply(lambda x: x.split("-")[0].strip().replace(' ','_').upper() if '-' in str(x) else str(x).replace(' ','_').upper())
    
    # Clean horse information
    if 'horse_name' in df.columns:
        # Extract horse ID (e.g., "RICH AND LUCKY (C413)" -> horse_id: C413, horse_name: RICH_AND_LUCKY)
        df['horse_id'] = df['horse_name'].apply(lambda x: x.split("(")[1].strip()[:-1] if '(' in str(x) and ')' in str(x) else 'Unknown')
        df['horse_name'] = df['horse_name'].apply(lambda x: x.split("(")[0].strip().replace(" ", "_") if '(' in str(x) else str(x).replace(" ", "_"))
    
    # Clean numeric columns
    numeric_cols = ['horse_no', 'actual_wt', 'declared_wt', 'draw', 'public_odds']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Clean placement column
    if 'plc' in df.columns:
        df['plc'] = pd.to_numeric(df['plc'], errors='coerce')
    
    return df

def load_progress_file():
    """Load the progress file to get the mapping of file numbers to dates"""
    date_mapping = {}
    
    if os.path.exists('progress.txt'):
        with open('progress.txt', 'r') as f:
            processed_dates = [line.strip() for line in f.readlines()]
        
        # Create mapping of file index to date
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
    
    # Get date mapping from progress file
    date_mapping = load_progress_file()
    
    # Store dataframes
    df_list = []
    
    # Find all race CSV files
    race_files = sorted(glob.glob('./races*.csv'), key=key_func)
    logging.info(f"Found {len(race_files)} race CSV files")
    
    if not race_files:
        logging.error("No race CSV files found! Make sure your scraper has run and created races*.csv files.")
        return None
    
    # Process each file
    for file_path in race_files:
        try:
            # Extract file number
            file_num = int(re.findall(r'races(\d+)\.csv', file_path)[0])
            
            logging.info(f"Processing {file_path}...")
            
            # Read CSV
            temp_df = pd.read_csv(file_path)
            
            if temp_df.empty:
                logging.warning(f"{file_path} is empty, skipping")
                continue
            
            # Add date column
            if file_num in date_mapping:
                temp_df["date"] = date_mapping[file_num]
                logging.info(f"Added date {date_mapping[file_num]} to {file_path}")
            else:
                # Try to extract date from the data itself or use filename
                extracted_date = extract_date_from_csv(temp_df)
                if extracted_date:
                    temp_df["date"] = extracted_date
                else:
                    temp_df["date"] = f"Unknown_{file_num}"
                    logging.warning(f"Could not determine date for {file_path}, using Unknown_{file_num}")
            
            # Clean the data
            temp_df = clean_race_data(temp_df)
            
            # Add file reference
            temp_df['source_file'] = file_path
            
            df_list.append(temp_df)
            logging.info(f"Successfully processed {file_path} with {len(temp_df)} rows")
            
        except Exception as e:
            logging.error(f"Error processing {file_path}: {e}")
            continue
    
    if not df_list:
        logging.error("No valid data found in any CSV files!")
        return None
    
    # Combine all dataframes
    logging.info("Combining all dataframes...")
    try:
        df = pd.concat(df_list, ignore_index=True, sort=False)
        logging.info(f"Combined dataset created with {len(df)} total rows")
        
        # Display summary information
        logging.info("="*50)
        logging.info("DATASET SUMMARY:")
        logging.info(f"Total races: {df['race_id'].nunique() if 'race_id' in df.columns else 'Unknown'}")
        logging.info(f"Total horses: {df['horse_id'].nunique() if 'horse_id' in df.columns else 'Unknown'}")
        logging.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
        logging.info(f"Total entries: {len(df)}")
        logging.info("="*50)
        
        # Display first few rows
        print("\nFirst 5 rows of cleaned data:")
        print(df.head())
        
        # Save the combined dataset
        output_file = 'combined_race_data.csv'
        df.to_csv(output_file, index=False)
        logging.info(f"Combined dataset saved as {output_file}")
        
        return df
        
    except Exception as e:
        logging.error(f"Error combining dataframes: {e}")
        return None

# Alternative function if you want to specify date range manually
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
    
    # Generate all dates in range
    all_dates = generate_date_range(start_date, end_date)
    
    # Create mapping for files that exist
    date_mapping_manual = {}
    file_count = 1
    
    for date in all_dates:
        if os.path.exists(f'races{file_count}.csv'):
            date_mapping_manual[file_count] = date
            file_count += 1
    
    logging.info(f"Created date mapping for {len(date_mapping_manual)} files")
    
    # Call main with manual date mapping
    return main_with_custom_mapping(date_mapping_manual)

def main_with_custom_mapping(custom_date_mapping):
    """Main processing function with custom date mapping"""
    logging.info("Starting data preparation with custom date mapping...")
    
    # Store dataframes
    df_list = []
    
    # Find all race CSV files
    race_files = sorted(glob.glob('./races*.csv'), key=key_func)
    logging.info(f"Found {len(race_files)} race CSV files")
    
    if not race_files:
        logging.error("No race CSV files found! Make sure your scraper has run and created races*.csv files.")
        return None
    
    # Process each file using custom mapping
    for file_path in race_files:
        try:
            file_num = int(re.findall(r'races(\d+)\.csv', file_path)[0])
            
            logging.info(f"Processing {file_path}...")
            temp_df = pd.read_csv(file_path)
            
            if temp_df.empty:
                logging.warning(f"{file_path} is empty, skipping")
                continue
            
            # Add date column using custom mapping
            if file_num in custom_date_mapping:
                temp_df["date"] = custom_date_mapping[file_num]
                logging.info(f"Added date {custom_date_mapping[file_num]} to {file_path}")
            else:
                temp_df["date"] = f"Unknown_{file_num}"
                logging.warning(f"Could not determine date for {file_path}, using Unknown_{file_num}")
            
            # Clean the data
            temp_df = clean_race_data(temp_df)
            temp_df['source_file'] = file_path
            df_list.append(temp_df)
            logging.info(f"Successfully processed {file_path} with {len(temp_df)} rows")
            
        except Exception as e:
            logging.error(f"Error processing {file_path}: {e}")
            continue
    
    if not df_list:
        logging.error("No valid data found in any CSV files!")
        return None
    
    # Combine all dataframes
    logging.info("Combining all dataframes...")
    try:
        df = pd.concat(df_list, ignore_index=True, sort=False)
        logging.info(f"Combined dataset created with {len(df)} total rows")
        
        # Save the combined dataset
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
        
        # Show column information
        print(f"\nColumns in final dataset:")
        for i, col in enumerate(df.columns):
            print(f"{i+1:2d}. {col}")
        
        # Show some basic statistics
        if 'date' in df.columns:
            print(f"\nUnique dates in dataset: {df['date'].nunique()}")
        if 'race_id' in df.columns:
            print(f"Unique races in dataset: {df['race_id'].nunique()}")
        if 'horse_id' in df.columns:
            print(f"Unique horses in dataset: {df['horse_id'].nunique()}")
    else:
        print("Data processing failed. Check the logs for errors.")