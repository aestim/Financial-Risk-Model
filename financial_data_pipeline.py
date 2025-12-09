import pandas as pd
import numpy as np
import glob
import os
import psycopg2
import requests 
from typing import List, Dict, Any, Tuple
from datetime import date

# ===============================================
# 1. Configuration and Connection Info
# ===============================================

# Database Connection Settings
DB_CONFIG = {
    'host': 'localhost', 
    'database': 'financial_db', 
    'user': 'min', 
    'password': 'your_local_password', # *** Update with your actual DB password ***
    'port': '5432'
}

# File paths and name patterns
DATA_PATH_PATTERN = "data/*q*" 
SUB_FILE_NAME = "sub.txt"
NUM_FILE_NAME = "num.txt"

# CIK-Ticker mapping data source URL and request headers (SEC compliance)
CIK_TICKER_MAP_URL = "https://www.sec.gov/include/ticker.txt" 
REQUEST_HEADERS = {'User-Agent': 'CIK Ticker Mapping Script (user.email@example.com)'} # Recommended to change to your actual email

# SEC XBRL Tag Mapping (Normalization)
CANONICAL_TAG_MAP = {
    # Z'' Score Ratios (X1-X5 Components)
    'Assets': ['Assets', 'AssetsNet'],
    'LiabilitiesCurrent': ['LiabilitiesCurrent', 'CurrentLiabilities'],
    'AssetsCurrent': ['AssetsCurrent', 'CurrentAssets'],
    'RetainedEarnings': ['RetainedEarnings', 'RetainedEarningsAccumulatedDeficit', 'AccumulatedDeficit'], 
    'EBIT': ['EarningsBeforeInterestAndTaxes', 'OperatingIncomeLoss', 'IncomeLossFromContinuingOperationsBeforeInterestExpenseAndTaxExpense'], 
    'Liabilities': ['Liabilities', 'TotalLiabilitiesNoncurrentAndCurrent'],
    'Revenues': ['Revenues', 'SalesRevenueNet'], 
    
    # Valuation Components (ROE, EPS)
    'NetIncomeLoss': ['NetIncomeLoss', 'IncomeLossFromContinuingOperations'], 
    'StockholdersEquity': ['StockholdersEquity', 'CommonStockholdersEquity'], 
    'CommonStockSharesOutstanding': ['CommonStockSharesOutstanding'], 
}

REQUIRED_CALCULATION_COLS = list(CANONICAL_TAG_MAP.keys()) 
ALL_POTENTIAL_TAGS = [tag for sublist in CANONICAL_TAG_MAP.values() for tag in sublist]
SUB_COLUMNS = ['adsh', 'cik', 'name', 'period', 'fy', 'fp'] 
NUM_COLUMNS = ['adsh', 'tag', 'ddate', 'value', 'uom', 'qtrs'] 


# ===============================================
# 2. CIK-Ticker Mapping Loader
# ===============================================

def load_cik_to_ticker_map() -> Dict[str, str]:
    """Downloads the latest CIK-to-Ticker mapping from the SEC and returns it as a dictionary."""
    print(f"INFO: Downloading CIK-to-Ticker map from {CIK_TICKER_MAP_URL}...")
    
    try:
        response = requests.get(CIK_TICKER_MAP_URL, headers=REQUEST_HEADERS, timeout=10)
        response.raise_for_status() 
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to download CIK-Ticker map. Using a small fallback map. Details: {e}")
        return {
            '320193': 'AAPL',
            '789019': 'MSFT',
        }

    cik_ticker_map = {}
    
    for line in response.text.strip().split('\n'):
        parts = line.split('\t')
        if len(parts) >= 2:
            ticker = parts[0].strip().upper()
            cik = parts[1].strip().lstrip('0') 
            
            if cik and ticker and cik not in cik_ticker_map:
                cik_ticker_map[cik] = ticker
                
    print(f"INFO: Successfully loaded {len(cik_ticker_map)} CIK-to-Ticker mappings.")
    return cik_ticker_map


# ===============================================
# 3. Data Loading and Merging
# ===============================================

def load_and_merge_data(quarter_path: str, cik_to_ticker_map: Dict[str, str]) -> pd.DataFrame:
    """Loads SUB and NUM data, normalizes tags, and merges to create a DataFrame."""
    
    sub_path = os.path.join(quarter_path, SUB_FILE_NAME)
    num_path = os.path.join(quarter_path, NUM_FILE_NAME)
    
    if not os.path.exists(sub_path) or not os.path.exists(num_path):
        print(f"WARNING: Skipping {quarter_path}. SUB or NUM file not found.")
        return pd.DataFrame()
        
    print(f"INFO: Loading data from {quarter_path}...")

    # 1. Load SUB data (Metadata)
    try:
        df_sub = pd.read_csv(sub_path, sep='\t', usecols=SUB_COLUMNS, encoding='utf-8', on_bad_lines='skip')
        df_sub = df_sub.rename(columns={'adsh': 'adsh'})
        df_sub['data_source'] = os.path.basename(quarter_path) 
    except Exception as e:
        print(f"ERROR loading SUB file in {quarter_path}: Details: {e}")
        return pd.DataFrame()

    # Apply CIK to Ticker mapping
    df_sub['cik'] = df_sub['cik'].astype(str)
    df_sub['ticker'] = df_sub['cik'].map(cik_to_ticker_map).fillna('UNKNOWN')
    
    # 2. Load NUM data (Financial values)
    try:
        df_num = pd.read_csv(num_path, sep='\t', usecols=NUM_COLUMNS, encoding='utf-8', on_bad_lines='skip')
    except Exception as e:
        print(f"ERROR loading NUM file in {quarter_path}: Details: {e}")
        return pd.DataFrame()

    # 3. Filter and Normalize NUM data with required tags
    df_num_filtered = df_num[df_num['tag'].isin(ALL_POTENTIAL_TAGS)].copy()
    
    if df_num_filtered.empty:
        print(f"WARNING: No relevant financial tags found in NUM file for {quarter_path}. Skipping merge.")
        return pd.DataFrame()

    tag_rename_map = {}
    for canonical_name, possible_tags in CANONICAL_TAG_MAP.items():
        for tag in possible_tags:
            tag_rename_map[tag] = canonical_name

    df_num_filtered['tag'] = df_num_filtered['tag'].map(tag_rename_map)
    
    # 4. Pivot NUM table (Convert to wide format)
    df_num_wide = df_num_filtered.pivot_table(
        index='adsh', 
        columns='tag', 
        values='value', 
        aggfunc='first' 
    ).reset_index()

    # 5. Merge SUB and NUM_WIDE
    df_merged = pd.merge(
        df_sub, 
        df_num_wide, 
        on='adsh', 
        how='inner'
    )
    
    return df_merged


# ===============================================
# 4. Z-Double Prime Score and Ratio Calculation
# ===============================================

def calculate_financial_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates Altman Z'' Score ratios (X1-X5), ROE, and EPS and applies rounding."""
    
    calculation_tags = REQUIRED_CALCULATION_COLS
    
    # 1. Remove rows missing any required financial item
    df_filtered = df.dropna(subset=calculation_tags, how='any').copy() 
    
    if df_filtered.empty:
        print("WARNING: All records filtered out due to missing required financial items.")
        return df_filtered

    # Convert values to numeric types
    for tag in calculation_tags:
        df_filtered[tag] = pd.to_numeric(df_filtered[tag], errors='coerce')

    # Prevent divide-by-zero errors (replace 0 with NaN for denominators)
    cols_to_replace_zero = ['Assets', 'Liabilities', 'CommonStockSharesOutstanding'] 
    df_filtered.loc[:, cols_to_replace_zero] = df_filtered[cols_to_replace_zero].replace(0, np.nan)
    
    # --- Z'' Score Ratios (X1-X5) Calculation ---
    df_filtered['X1'] = (df_filtered['AssetsCurrent'] - df_filtered['LiabilitiesCurrent']) / df_filtered['Assets']
    df_filtered['X2'] = df_filtered['RetainedEarnings'] / df_filtered['Assets']
    df_filtered['X3'] = df_filtered['EBIT'] / df_filtered['Assets']
    df_filtered['X4'] = df_filtered['StockholdersEquity'] / df_filtered['Liabilities']
    df_filtered['X5'] = df_filtered['Revenues'] / df_filtered['Assets']
    
    # Calculate Z'' Score
    df_filtered['z_score'] = (
        6.56 * df_filtered['X1'] + 
        3.26 * df_filtered['X2'] + 
        6.72 * df_filtered['X3'] + 
        1.05 * df_filtered['X4']
    )
    
    # --- Valuation Ratios (ROE, EPS) ---
    df_filtered['roe'] = df_filtered['NetIncomeLoss'] / df_filtered['StockholdersEquity']
    df_filtered['eps'] = df_filtered['NetIncomeLoss'] / df_filtered['CommonStockSharesOutstanding']

    # Apply Rounding based on analysis standards
    df_filtered['X1'] = df_filtered['X1'].round(4)
    df_filtered['X2'] = df_filtered['X2'].round(4)
    df_filtered['X3'] = df_filtered['X3'].round(4)
    df_filtered['X4'] = df_filtered['X4'].round(4)
    df_filtered['X5'] = df_filtered['X5'].round(4)
    
    df_filtered['z_score'] = df_filtered['z_score'].round(3) 
    df_filtered['roe'] = df_filtered['roe'].round(4)
    df_filtered['eps'] = df_filtered['eps'].round(2) 

    # Z'' Score Interpretation and Prediction
    def get_prediction(z):
        if pd.isna(z):
            return 'Incomplete/Invalid'
        elif z > 2.6: 
            return 'Safe' 
        elif z > 1.1: 
            return 'Grey' 
        else: 
            return 'Distress'

    df_filtered['prediction'] = df_filtered['z_score'].apply(get_prediction)
    
    # FIX: Replace Z-Score NaN with 0.0 if the prediction is 'Incomplete/Invalid'
    df_filtered.loc[
        df_filtered['z_score'].isna() & (df_filtered['prediction'] == 'Incomplete/Invalid'), 
        'z_score'
    ] = 0.0
    
    return df_filtered


# ===============================================
# 5. Database Storage Functions
# ===============================================

def get_db_connection():
    """Returns a PostgreSQL connection object."""
    safe_config = {k: v for k, v in DB_CONFIG.items() if k != 'password'}
    print(f"INFO: Attempting to connect to DB: {safe_config}")
    return psycopg2.connect(**DB_CONFIG)

def safe_to_insert_list(results: List[Dict[str, Any]]) -> List[Tuple]:
    """
    Creates a list of tuples exactly matching the DB schema order (excluding 'id'), 
    converting pandas NaN to Python None for safe insertion into numeric DB fields.
    Also converts large magnitude fields to string to prevent 'integer out of range'.
    """
    data_to_insert = []
    
    # Helper function to handle large numeric values safely
    def safe_numeric_to_db(value):
        if pd.isna(value) or value is None:
            return None
        # Convert to string to avoid psycopg2 guessing a native type that overflows DB INTEGER
        return str(value) 

    for r in results:
        fy_val = r.get('fy')
        
        # 🚨 FIX APPLIED HERE: Ensure fiscal_year is a clean integer (or None if NaN/None)
        if pd.isna(fy_val) or fy_val is None:
            safe_fy = None
        else:
            # Convert float (e.g., 2022.0) to integer (2022) to prevent float-to-int DB casting issues
            safe_fy = int(fy_val) 
            
        # DB Schema Order (20 columns, excluding auto-generated 'id'):
        # ... (list order confirmed previously)
        
        row = (
            r.get('adsh'), 
            r.get('cik'), 
            r.get('name'), 
            r.get('ticker'), 
            r.get('period'),         # date object
            r.get('fp'),             # fiscal_period (text)
            safe_fy,                 # <--- USING FIXED FY VALUE
            
            # X1-X5 (Ratios, handled as float/None)
            None if pd.isna(r.get('x1_wcta')) else r.get('x1_wcta'),
            None if pd.isna(r.get('x2_reta')) else r.get('x2_reta'),
            None if pd.isna(r.get('x3_ebitta')) else r.get('x3_ebitta'),
            None if pd.isna(r.get('x4_mvtl')) else r.get('x4_mvtl'),
            None if pd.isna(r.get('x5_salesta')) else r.get('x5_salesta'),
            
            # Large Magnitude Fields (Converted to string for DB safety)
            safe_numeric_to_db(r.get('net_income')),
            safe_numeric_to_db(r.get('total_equity')),
            safe_numeric_to_db(r.get('shares_outstanding')),
            
            r.get('z_score'),       # z_score is guaranteed non-NaN/None
            r.get('prediction'),    # text
            None if pd.isna(r.get('eps')) else r.get('eps'),
            None if pd.isna(r.get('roe')) else r.get('roe'),
            r.get('data_source')    # text
        )
        data_to_insert.append(row)
        
    return data_to_insert

def save_results_to_db(results: List[Dict[str, Any]]):
    """Saves the analysis results to the financial_analysis_results table (Upsert)."""
    
    # The INSERT query must include exactly 20 columns, excluding the auto-generated 'id'
    insert_query = """
    INSERT INTO financial_analysis_results ( 
        adsh, cik, name, ticker, period, fiscal_period, fiscal_year,
        x1_wcta, x2_reta, x3_ebitta, x4_mvtl, x5_salesta, 
        net_income, total_equity, shares_outstanding, 
        z_score, prediction, eps, roe, data_source
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (adsh, period) 
    DO UPDATE SET 
        z_score = EXCLUDED.z_score, 
        prediction = EXCLUDED.prediction,
        eps = EXCLUDED.eps,
        roe = EXCLUDED.roe,
        data_source = EXCLUDED.data_source;
    """
    
    conn = None
    data_to_insert = []
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Use the custom function to ensure correct order, column count (20), and type safety
        data_to_insert = safe_to_insert_list(results)
        
        cur.executemany(insert_query, data_to_insert)
        conn.commit()
        print(f"INFO: Successfully saved {len(data_to_insert)} analysis results to DB.")
        
    except (Exception, psycopg2.Error) as error:
        print("="*50)
        print(f"FATAL ERROR: Failed during DB saving. (Check column count, order, or data types)")
        print(f"Details: {error}")
        
        # DEBUGGING FEATURE: Print all data that was being inserted when the error occurred
        if data_to_insert:
            print("\n--- DEBUG: Full Data Being Inserted (First 5 Rows) ---")
            
            col_names = ['adsh', 'cik', 'name', 'ticker', 'period', 'fp', 'fy',
                         'x1_wcta', 'x2_reta', 'x3_ebitta', 'x4_mvtl', 'x5_salesta',
                         'net_income', 'total_equity', 'shares_outstanding',
                         'z_score', 'prediction', 'eps', 'roe', 'data_source']
            print(col_names)

            for i, row in enumerate(data_to_insert):
                if i >= 5: # Limit to first 5 rows to prevent massive output
                    print(f"... and {len(data_to_insert) - 5} more rows.")
                    break
                print(row)
            print("-----------------------------------------------------")
        
        print("="*50)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


# ===============================================
# 6. Main Execution Function
# ===============================================

def main_process():
    """Reads all files, calculates Z'' Score, and saves to the DB."""
    
    # 1. Load CIK-Ticker Map
    cik_to_ticker_map = load_cik_to_ticker_map()
    
    # 2. Search all quarter directories
    quarter_dirs = glob.glob(DATA_PATH_PATTERN) 
    
    if not quarter_dirs:
        print("ERROR: No quarter directories found in the specified path (e.g., 'data/2023q1'). Exiting.")
        return

    print(f"INFO: Found {len(quarter_dirs)} quarter directories. Starting process.")
    
    all_combined_data = []
    
    # Iterate through each quarter directory to load/merge data
    for q_dir in quarter_dirs:
        df_combined = load_and_merge_data(q_dir, cik_to_ticker_map) 
        if not df_combined.empty:
            all_combined_data.append(df_combined)

    if not all_combined_data:
        print("WARNING: No data successfully loaded and combined across all directories. Exiting.")
        return

    df_final = pd.concat(all_combined_data, ignore_index=True)
    
    # Date format correction (YYYYMMDD to date object)
    try:
        df_final['period'] = df_final['period'].astype('Int64', errors='ignore').astype(str)
        df_final['period'] = pd.to_datetime(df_final['period'], format='%Y%m%d', errors='coerce').dt.date
    except Exception as e:
        print(f"WARNING: Could not fully convert 'period' column to date objects: {e}.")
    
    df_final.dropna(subset=['period'], inplace=True) 
    
    print(f"INFO: Total combined records for analysis: {len(df_final)}")
    
    # 3. Calculate Z-Score (Z'') and Valuation Ratios
    df_results = calculate_financial_ratios(df_final)
    
    if df_results.empty:
        print("WARNING: No final results remaining after ratio calculation. Exiting.")
        return

    print(f"INFO: Total calculated results ready for DB insertion: {len(df_results)}")

    # 4. Format data for DB storage
    # Rename calculated columns to match DB schema names
    df_results_subset = df_results.rename(columns={
        'X1': 'x1_wcta', 'X2': 'x2_reta', 'X3': 'x3_ebitta', 'X4': 'x4_mvtl', 'X5': 'x5_salesta',
        'NetIncomeLoss': 'net_income', 'StockholdersEquity': 'total_equity', 
        'CommonStockSharesOutstanding': 'shares_outstanding',
    })

    # Prepare data for DB insertion using the exact DB column order (excluding 'id')
    results_list = df_results_subset[
        ['adsh', 'cik', 'name', 'ticker', 'period', 'fp', 'fy',
         'x1_wcta', 'x2_reta', 'x3_ebitta', 'x4_mvtl', 'x5_salesta',
         'net_income', 'total_equity', 'shares_outstanding',
         'z_score', 'prediction', 'eps', 'roe', 'data_source']
    ].to_dict('records')

    # 5. Save to DB
    if results_list:
        save_results_to_db(results_list)

if __name__ == "__main__":
    main_process()