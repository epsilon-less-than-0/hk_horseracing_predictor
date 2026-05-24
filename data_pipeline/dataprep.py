import pandas as pd
import numpy as np
import glob
import re, os
import logging
import math
import sqlite3
import networkx as nx

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MarginAdjustedEloEngine:
    def __init__(self, k_base=20.0, initial_elo=1500.0):
        self.k_base = k_base
        self.initial_elo = initial_elo

    def _parse_lbw(self, lbw_str):
        if pd.isna(lbw_str) or str(lbw_str).strip() in ['---', '-', '']: return 0.0
        s = str(lbw_str).strip().upper()
        if s in ['N', 'NOSE']: return 0.05
        if s in ['SH', 'SHD', 'SN']: return 0.1
        if s in ['HD']: return 0.2
        if s in ['DH']: return 0.0
        try:
            if '-' in s:
                parts = s.split('-')
                return float(parts[0]) + (float(parts[1].split('/')[0]) / float(parts[1].split('/')[1]))
            elif '/' in s: return float(s.split('/')[0]) / float(s.split('/')[1])
            else: return float(s)
        except Exception: return 0.0

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Phase 27: Engineering Margin-Adjusted Pairwise Elo...")
        df['parsed_lbw'] = df['lbw'].apply(self._parse_lbw)
        df['plc'] = pd.to_numeric(df['plc'], errors='coerce').fillna(99.0)

        df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
        df = df.sort_values(by=['date', 'race_id', 'plc']).reset_index(drop=True)
        
        elo_dict = {}
        df['pre_race_elo'] = self.initial_elo 
        grouped_races = df.groupby(['date', 'race_id'], sort=False) 
        
        for (r_date, r_id), race_data in grouped_races:
            indices = race_data.index.tolist()
            horses = race_data['horse_id'].tolist()
            positions = race_data['plc'].tolist()
            margins = race_data['parsed_lbw'].tolist()
            
            current_race_elos = []
            for idx, horse in zip(indices, horses):
                if horse not in elo_dict: elo_dict[horse] = self.initial_elo
                current_elo = elo_dict[horse]
                current_race_elos.append(current_elo)
                df.at[idx, 'pre_race_elo'] = current_elo 
                
            num_horses = len(horses)
            updates = {horse: 0.0 for horse in horses}
            
            if num_horses > 1:
                for i in range(num_horses):
                    for j in range(num_horses):
                        if i == j: continue
                        horse_a, horse_b = horses[i], horses[j]
                        pos_a, pos_b = positions[i], positions[j]
                        margin_a, margin_b = margins[i], margins[j]
                        elo_a, elo_b = current_race_elos[i], current_race_elos[j]
                        
                        s_a = 1.0 if pos_a < pos_b else (0.0 if pos_a > pos_b else 0.5)
                        e_a = 1.0 / (1.0 + math.pow(10, (elo_b - elo_a) / 400.0))
                        
                        movm = 1.0 + math.log(abs(margin_a - margin_b) + 1.0)
                        updates[horse_a] += ((self.k_base * movm) * (s_a - e_a)) / (num_horses - 1)
                    
            for horse in horses: elo_dict[horse] += updates[horse]
        return df

class SectionalPaceEngineer:
    def __init__(self, rolling_window=5):
        self.rolling_window = rolling_window

    def _parse_running_pos(self, pos_string):
        if pd.isna(pos_string) or pos_string == '---': return []
        return [int(x) for x in str(pos_string).split() if x.isdigit()]

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Phase 8: Engineering Sectional Pace Momentum...")
        parsed_pos = df['running_pos'].apply(self._parse_running_pos)
        
        df['raw_ESI'] = parsed_pos.apply(lambda x: 1.0 / np.sqrt(x[0]) if len(x) > 0 and x[0] > 0 else np.nan)
        df['raw_CSI'] = parsed_pos.apply(lambda x: (x[-2] - x[-1]) if len(x) >= 2 else 0)
        
        df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
        df = df.sort_values(by=['horse_id', 'date']).reset_index(drop=True)
        
        df['shifted_rolling_ESI'] = df.groupby('horse_id')['raw_ESI'].transform(
            lambda x: x.shift(1).rolling(window=self.rolling_window, min_periods=1).mean()
        )
        df['shifted_rolling_CSI'] = df.groupby('horse_id')['raw_CSI'].transform(
            lambda x: x.shift(1).rolling(window=self.rolling_window, min_periods=1).mean()
        )
        
        top_3_esi = df.groupby('race_id')['shifted_rolling_ESI'].apply(lambda x: x.nlargest(3).sum()).reset_index()
        top_3_esi.rename(columns={'shifted_rolling_ESI': 'race_ESI_pressure'}, inplace=True)
        
        df = pd.merge(df, top_3_esi, on='race_id', how='left')
        df['pace_advantage'] = df['shifted_rolling_ESI'] - df['race_ESI_pressure']
        return df

class HumanMomentumEngineer:
    def __init__(self, rolling_window=30):
        self.rolling_window = rolling_window

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Phase 29: Engineering Human Momentum...")
        df['temp_is_win'] = (df['plc'] == 1.0).astype(int)

        df = df.sort_values(by=['jockey_name', 'date', 'race_id']).reset_index(drop=True)
        df['jockey_win_pct'] = df.groupby('jockey_name')['temp_is_win'].transform(
            lambda x: x.shift(1).rolling(window=self.rolling_window, min_periods=1).mean()
        )
        
        df = df.sort_values(by=['trainer_name', 'date', 'race_id']).reset_index(drop=True)
        df['trainer_win_pct'] = df.groupby('trainer_name')['temp_is_win'].transform(
            lambda x: x.shift(1).rolling(window=self.rolling_window, min_periods=1).mean()
        )

        df['jockey_win_pct'] = df['jockey_win_pct'].fillna(0.083)
        df['trainer_win_pct'] = df['trainer_win_pct'].fillna(0.083)
        df.drop(columns=['temp_is_win'], inplace=True)
        return df

class Glicko2Engineer:
    def __init__(self, tau=0.5):
        self.tau = tau
        self.GLICKO_SCALE = 173.7178
        self.INIT_RATING = 1500.0
        self.INIT_RD = 350.0
        self.INIT_VOL = 0.06
        self.ratings, self.rds, self.vols = {}, {}, {}

    def _g2_transform(self, r, rd): return (r - self.INIT_RATING) / self.GLICKO_SCALE, rd / self.GLICKO_SCALE
    def _g_phi(self, phi): return 1.0 / math.sqrt(1.0 + 3.0 * phi**2 / (math.pi**2))
    def _E(self, mu, mu_j, phi_j): return 1.0 / (1.0 + math.exp(-self._g_phi(phi_j) * (mu - mu_j)))

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Phase 32: Engineering Glicko-2 Biological Volatility...")
        df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
        df = df.sort_values(by=['date', 'race_id', 'plc']).reset_index(drop=True)
        
        df['pre_race_glicko_mu'] = self.INIT_RATING
        df['pre_race_glicko_rd'] = self.INIT_RD
        df['pre_race_glicko_vol'] = self.INIT_VOL
        
        grouped_races = df.groupby(['date', 'race_id'], sort=False)
        for (r_date, r_id), race_data in grouped_races:
            indices = race_data.index.tolist()
            horses = race_data['horse_id'].tolist()
            positions = race_data['plc'].fillna(99.0).tolist()
            
            for idx, horse in zip(indices, horses):
                if horse not in self.ratings:
                    self.ratings[horse], self.rds[horse], self.vols[horse] = self.INIT_RATING, self.INIT_RD, self.INIT_VOL
                df.at[idx, 'pre_race_glicko_mu'] = self.ratings[horse]
                df.at[idx, 'pre_race_glicko_rd'] = self.rds[horse]
                df.at[idx, 'pre_race_glicko_vol'] = self.vols[horse]
            
            num_horses = len(horses)
            if num_horses < 2: continue
            updates = {}
            for i in range(num_horses):
                horse_a, pos_a = horses[i], positions[i]
                mu_a, phi_a = self._g2_transform(self.ratings[horse_a], self.rds[horse_a])
                vol_a = self.vols[horse_a]
                
                v_inv, delta_sum = 0.0, 0.0
                for j in range(num_horses):
                    if i == j: continue
                    horse_b, pos_b = horses[j], positions[j]
                    mu_b, phi_b = self._g2_transform(self.ratings[horse_b], self.rds[horse_b])
                    s = 1.0 if pos_a < pos_b else (0.0 if pos_a > pos_b else 0.5)
                    g_j = self._g_phi(phi_b)
                    e_j = self._E(mu_a, mu_b, phi_b)
                    
                    v_inv += (g_j**2) * e_j * (1.0 - e_j)
                    delta_sum += g_j * (s - e_j)
                
                v = 1.0 / v_inv if v_inv > 0 else 1.0
                phi_star = math.sqrt(phi_a**2 + vol_a**2)
                phi_prime = 1.0 / math.sqrt(1.0 / phi_star**2 + 1.0 / v)
                mu_prime = mu_a + (phi_prime**2) * delta_sum
                updates[horse_a] = {
                    'r': (phi_prime * self.GLICKO_SCALE + self.INIT_RATING) if mu_prime == mu_a else (mu_prime * self.GLICKO_SCALE + self.INIT_RATING),
                    'rd': phi_prime * self.GLICKO_SCALE
                }

            for horse, data in updates.items():
                self.ratings[horse], self.rds[horse] = data['r'], data['rd']
        return df

class EquineNetworkEngineer:
    """
    Phase 36: Topological Data Analysis (TDA).
    Constructs a Look-Ahead-Free Directed Graph of the HKJC ecosystem.
    Calculates Eigenvector Centrality (PageRank) to capture indirect dominance networks.
    """
    def __init__(self, damping=0.85):
        self.damping = damping
        self.graph = nx.DiGraph()
        self.default_pr = 1.0 / 1000.0 # Baseline nominal rank

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Phase 36: Engineering Topological Network Centrality (PageRank)...")
        
        df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
        df = df.sort_values(by=['date', 'race_id', 'plc']).reset_index(drop=True)
        
        df['pre_race_pagerank'] = self.default_pr
        grouped_dates = df.groupby('date', sort=True)
        
        for date, day_data in grouped_dates:
            # 1. Snapshot Centrality BEFORE the day's races to prevent Leakage
            if len(self.graph) > 0:
                try:
                    current_pagerank = nx.pagerank(self.graph, alpha=self.damping, weight='weight')
                except Exception:
                    current_pagerank = {}
            else:
                current_pagerank = {}
                
            indices = day_data.index.tolist()
            horses = day_data['horse_id'].tolist()
            
            for idx, horse in zip(indices, horses):
                df.at[idx, 'pre_race_pagerank'] = current_pagerank.get(horse, self.default_pr)
            
            # 2. Update the Directed Graph with the day's results
            grouped_races = day_data.groupby('race_id', sort=False)
            for r_id, race in grouped_races:
                race_horses = race['horse_id'].tolist()
                positions = race['plc'].fillna(99.0).tolist()
                
                for i in range(len(race_horses)):
                    for j in range(len(race_horses)):
                        if i == j: continue
                        
                        horse_a, pos_a = race_horses[i], positions[i]
                        horse_b, pos_b = race_horses[j], positions[j]
                        
                        # If A beats B, influence flows from B -> A
                        if pos_a < pos_b:
                            if self.graph.has_edge(horse_b, horse_a):
                                self.graph[horse_b][horse_a]['weight'] += 1.0
                            else:
                                self.graph.add_edge(horse_b, horse_a, weight=1.0)
                                
        return df

def key_func(afilename): return int(re.compile("\\D").sub("", afilename))
def extract_date_from_csv(df):
    try:
        for col in df.columns:
            if 'date' in str(col).lower(): return df[col].iloc[0] if not df.empty else None
        return None
    except: return None

def clean_race_data(df):
    if df.empty: return df
    num_cols = len(df.columns)
    expected_cols = ['race_name', 'going', 'race_type', 'plc', 'horse_no', 'horse_name', 
                    'jockey_name', 'trainer_name', 'actual_wt', 'declared_wt', 
                    'draw', 'lbw', 'running_pos', 'finish_time', 'public_odds']
    
    if num_cols >= len(expected_cols): df.columns = expected_cols + [f'extra_col_{i}' for i in range(num_cols - len(expected_cols))]
    else: df.columns = expected_cols[:num_cols]
    
    if 'race_name' in df.columns:
        df['race_id'] = df['race_name'].apply(lambda x: x.split('(')[1][:-1] if '(' in str(x) and ')' in str(x) else 'Unknown')
    if 'race_type' in df.columns:
        df['race_dist'] = df['race_type'].apply(lambda x: x.split("-")[1].strip().replace(' ','_').upper().rstrip('M') if '-' in str(x) else 'Unknown')
    if 'horse_name' in df.columns:
        df['horse_id'] = df['horse_name'].apply(lambda x: x.split("(")[1].strip()[:-1] if '(' in str(x) and ')' in str(x) else 'Unknown')
    
    for col in ['horse_no', 'actual_wt', 'declared_wt', 'draw', 'public_odds']:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
    if 'plc' in df.columns: df['plc'] = pd.to_numeric(df['plc'], errors='coerce')
    return df

def load_progress_file():
    date_mapping = {}
    if os.path.exists('progress.txt'):
        with open('progress.txt', 'r') as f: processed_dates = [line.strip() for line in f.readlines()]
        for i, date in enumerate(processed_dates, 1):
            if os.path.exists(f'races{i}.csv'): date_mapping[i] = date
    return date_mapping

def main():
    logging.info("=== INITIALIZING V12 DATA PIPELINE (Equine PageRank Integration) ===")
    date_mapping = load_progress_file()
    df_list = []
    
    race_files = sorted(glob.glob('./races*.csv'), key=key_func)
    if not race_files: return None
    
    for file_path in race_files:
        try:
            file_num = int(re.findall(r'races(\d+)\.csv', file_path)[0])
            temp_df = pd.read_csv(file_path)
            if temp_df.empty: continue
            
            temp_df = clean_race_data(temp_df)
            if file_num in date_mapping: temp_df["date"] = date_mapping[file_num]
            else:
                extracted = extract_date_from_csv(temp_df)
                temp_df["date"] = extracted if extracted else f"Unknown_{file_num}"
            df_list.append(temp_df)
        except Exception: continue
    
    if not df_list: return None
    
    try:
        df = pd.concat(df_list, ignore_index=True, sort=False)
        
        df = SectionalPaceEngineer(rolling_window=5).fit_transform(df)
        df = MarginAdjustedEloEngine().fit_transform(df)
        df = HumanMomentumEngineer(rolling_window=30).fit_transform(df)
        df = Glicko2Engineer().fit_transform(df)
        
        # --- PHASE 36: TOPOLOGICAL NETWORK INJECTION (PageRank) ---
        df = EquineNetworkEngineer().fit_transform(df)
        
        conn = sqlite3.connect('hk_racing.db')
        df.to_sql('ml_features_v12', conn, if_exists='replace', index=False)
        conn.close()
        
        logging.info("V12 Matrix Compilation Complete. Output saved to SQLite: ml_features_v12")
        return df
        
    except Exception as e:
        logging.error(f"Error compiling V12 Matrix: {e}")
        return None

if __name__ == "__main__":
    main()