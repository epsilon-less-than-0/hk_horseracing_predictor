# Hong Kong Jockey Club Race Data Scraper and Analysis

Currently a scraping and data processing program for Hong Kong Jockey Club horse racing results. Automatically collects historical race data and transforms it into clean, analysis-ready datasets. This project's aim is to analyze this data and  predict race results, and eventually come up with optimal betting strategies.

## Installation

### Prerequisites
- Python 3.7+
- Firefox browser
- GeckoDriver for Selenium

### Install Dependencies
```bash
pip install selenium pandas beautifulsoup4 requests numpy matplotlib
```

### Download GeckoDriver
1. Download from [Mozilla's GeckoDriver releases](https://github.com/mozilla/geckodriver/releases)
2. Place `geckodriver.exe` in your system PATH or project folder

## Starting

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/hk-horseracing-scraper.git
cd hk-horseracing-scraper
```

### 2. Run the Scraper (Test Mode)
```bash
python3 datascraper.py
```
The scraper starts with a small test dataset (10 dates around Christmas 2019) to verify everything works.

### 3. Process the Data
```bash
python3 dataprep.py
```
This combines and cleans all scraped CSV files into `combined_race_data.csv`.

### 4. Expand to Full Dataset
Once testing is successful:
1. Edit `datascraper.py`
2. Uncomment the full dates list or modify the date range
3. Re-run the scraper for complete historical data

## Usage

### Basic Scraping

```python
# The scraper automatically:
# 1. Checks each date for race data
# 2. Skips dates with no races  
# 3. Extracts comprehensive race information
# 4. Saves progress for resuming later

# Configuration (edit in script):
START_DATE = "01/01/2018"  # Customize start date
END_DATE = "31/12/2019"    # Customize end date
```
