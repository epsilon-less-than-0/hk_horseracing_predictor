import sqlite3
import pandas as pd

def check_schema():
    conn = sqlite3.connect('hk_racing.db')
    
    print("=== TABLES IN VAULT ===")
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
    table_names = tables['name'].tolist()
    print(table_names)
    
    for table in table_names:
        if table in ['race_results', 'race_info', 'races', 'ml_features']:
            print(f"\n=== COLUMNS IN {table.upper()} ===")
            columns = pd.read_sql(f"PRAGMA table_info({table});", conn)
            print(columns['name'].tolist())
            
    conn.close()

if __name__ == "__main__":
    check_schema()