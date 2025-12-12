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
# TODO: Modify DB_CONFIG to match your environment.
DB_CONFIG = {
    'host': 'localhost', 
    'database': 'financial_db', 
    'user': 'min', 
    'password': 'your_local_password', # Replace with your actual password
    'port': '5432'
}

# File paths and name patterns
DATA_PATH_PATTERN = "data/*q*" 
SUB_FILE_NAME = "sub.txt"
NUM_FILE_NAME = "num.txt"

# CIK-Ticker mapping data source URL and request headers (SEC compliance)
CIK_TICKER_MAP_URL = "https://www.sec.gov/include/ticker.txt" 
REQUEST_HEADERS = {'User-Agent': 'CIK Ticker Mapping Script (user.email@example.com)'} 

# SEC XBRL Tag Mapping (Normalization) - EXPANDED BASED ON DIAGNOSIS
CANONICAL_TAG_MAP = {
    # Z'' Score CORE Tags 
    'Assets': ['Assets', 'AssetsTotal', 'AssetsNet', 'TotalAssets', 'AssetsCombined'],
    
    # FIX: Added aliases for LiabilitiesCurrent
    'LiabilitiesCurrent': [
        'LiabilitiesCurrent', 'CurrentLiabilities', 'LiabilitiesCurrentTotal', 'TotalCurrentLiabilities', 
        'LiabilitiesCurrentAbstract', 'TotalLiabilitiesCurrent'
    ], 
    
    # FIX: Added aliases for AssetsCurrent
    'AssetsCurrent': [
        'AssetsCurrent', 'CurrentAssets', 'AssetsCurrentTotal','TotalCurrentAssets', 
        'AssetsCurrentAbstract', 'TotalAssetsCurrent'
    ], 
    
    # FIX: Added aliases for RetainedEarnings
    'RetainedEarnings': [
        'RetainedEarnings', 'AccumulatedDeficit', 'RetainedEarningsAccumulatedDeficit',
        'RetainedEarningsAccumulatedDeficitAbstract'
    ], 
    
    # EBIT: Tags for Operating Income
    'EBIT': [
        'OperatingIncomeLoss', 'EarningsBeforeInterestAndTaxes', 
        'IncomeLossFromContinuingOperationsBeforeInterestExpenseAndTaxExpense', 
        'IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityInvestments',
        'PretaxIncomeLossFromContinuingOperations', 
    ], 
    
    'Liabilities': ['Liabilities', 'LiabilitiesTotal', 'TotalLiabilitiesNoncurrentAndCurrent','TotalLiabilities'],
    
    # FIX: Added the suggested tag for Revenues (RevenueFromContractWithCustomerIncludingAssessedTax)
    'Revenues': [
        'Revenues', 'SalesRevenueNet', 'RevenuesTotal', 'TotalRevenue', 'NetSales', 
        'RevenueFromContractWithCustomerExcludingAssessedTax', 
        'RevenueFromContractWithCustomerIncludingAssessedTax' 
    ], 
    
    'StockholdersEquity': ['StockholdersEquity', 'CommonStockholdersEquity', 'PartnersCapital', 'TotalStockholdersEquity', 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest', 'EquityAttributableToParent'], 
    
    # Valuation Components
    'NetIncomeLoss': ['NetIncomeLoss', 'IncomeLossFromContinuingOperations', 'ProfitLoss', 'NetIncomeLossAttributableToParent'], 
    'CommonStockSharesOutstanding': ['CommonStockSharesOutstanding', 'CommonStockSharesIssued', 'WeightedAverageNumberOfShareOutstandingBasic'],
}

Z_SCORE_CORE_TAGS = [
    'Assets', 'LiabilitiesCurrent', 'AssetsCurrent', 'RetainedEarnings', 
    'EBIT', 'Liabilities', 'Revenues', 'StockholdersEquity'
]

ALL_CANONICAL_TAGS = list(CANONICAL_TAG_MAP.keys())
ALL_POTENTIAL_TAGS = [tag for sublist in CANONICAL_TAG_MAP.values() for tag in sublist]
SUB_COLUMNS_REQUIRED = ['adsh', 'cik', 'name', 'period', 'fy', 'fp'] 
NUM_COLUMNS = ['adsh', 'tag', 'ddate', 'value', 'uom', 'qtrs', 'dim'] 

def get_tag_rename_map(canonical_map: Dict[str, List[str]]) -> Dict[str, str]:
    """Creates a reverse tag rename map from the canonical map."""
    tag_rename_map = {}
    for canonical_name, possible_tags in canonical_map.items():
        for tag in possible_tags:
            tag_rename_map[tag] = canonical_name
    return tag_rename_map

# ===============================================
# 2. CIK-Ticker Mapping Loader
# ===============================================

def load_cik_to_ticker_map() -> Dict[str, str]:
    """Downloads the latest CIK-to-Ticker mapping from the SEC."""
    print(f"INFO: Downloading CIK-to-Ticker map from {CIK_TICKER_MAP_URL}...")
    try:
        response = requests.get(CIK_TICKER_MAP_URL, headers=REQUEST_HEADERS, timeout=10)
        response.raise_for_status() 
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to download CIK-Ticker map. Details: {e}")
        return {} 

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
        return pd.DataFrame()
        
    # 1. Load SUB data (Metadata)
    df_sub = pd.read_csv(sub_path, sep='\t', usecols=SUB_COLUMNS_REQUIRED, encoding='utf-8', on_bad_lines='skip', low_memory=False)
    df_sub = df_sub.rename(columns={'adsh': 'adsh'})
    df_sub['data_source'] = os.path.basename(quarter_path) 
    
    # Filter for Annual reports only (FP='FY')
    df_sub = df_sub[df_sub['fp'] == 'FY'].copy()
    if df_sub.empty: return pd.DataFrame()

    # Apply CIK to Ticker mapping
    df_sub['cik'] = df_sub['cik'].astype('Int64').astype(str)
    
    # --- Force convert 'fy' column to Int64 to remove .0 and maintain Nullable Int (Stability Improvement) ---
    try:
        df_sub['fy'] = pd.to_numeric(df_sub['fy'], errors='coerce').astype('Int64')
    except Exception:
        # Keep as float if Int64 conversion fails
        pass 
    # --------------------------------------------------------------------------------------------------------
        
    df_sub['ticker'] = df_sub['cik'].map(cik_to_ticker_map).fillna('UNKNOWN')
    
    # 2. Load NUM data (Financial values)
    df_num = pd.read_csv(
        num_path, sep='\t', usecols=range(len(NUM_COLUMNS)), header=None, names=NUM_COLUMNS, encoding='utf-8', on_bad_lines='skip', low_memory=False
    )

    # 3. Context (dim) Filtering and Tag Normalization
    df_num_filtered = df_num[df_num['dim'].isna()].copy()
    df_num_filtered = df_num_filtered[df_num_filtered['tag'].isin(ALL_POTENTIAL_TAGS)].copy()
    
    if df_num_filtered.empty: return pd.DataFrame()

    tag_rename_map = get_tag_rename_map(CANONICAL_TAG_MAP)
    df_num_filtered['tag'] = df_num_filtered['tag'].map(tag_rename_map)
    
    # 4. Pivot NUM table (Convert to wide format)
    df_num_wide = df_num_filtered.pivot_table(index='adsh', columns='tag', values='value', aggfunc='first').reset_index()

    # 5. Merge SUB and NUM_WIDE
    df_merged = pd.merge(df_sub, df_num_wide, on='adsh', how='inner')
    
    return df_merged


# ===============================================
# 4. Z-Double Prime Score and Ratio Calculation
# ===============================================

# ===============================================
# 4. Z-Double Prime Score and Ratio Calculation - (UPDATED)
# ===============================================

def calculate_financial_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates Altman Z'' Score ratios (X1-X5), ROE, and EPS and applies rounding."""
    
    # Z'' Score의 최종 계산 (X1~X4)에 필요한 핵심 7개 태그만 정의하여 데이터 제약을 완화합니다.
    required_tags_for_z_score = [
        'Assets', 'LiabilitiesCurrent', 'AssetsCurrent', 'RetainedEarnings', 
        'EBIT', 'Liabilities', 'StockholdersEquity'
    ]
    
    # 7개 필수 태그 중 하나라도 누락되면 해당 레코드를 제외합니다. (Revenues 제외)
    df_filtered = df.dropna(subset=required_tags_for_z_score, how='any').copy() 
    
    if df_filtered.empty:
        print("WARNING: All records filtered out due to missing required financial items.")
        return df_filtered

    # Convert all necessary tags to numeric types
    for tag in ALL_CANONICAL_TAGS:
        if tag in df_filtered.columns:
             df_filtered[tag] = pd.to_numeric(df_filtered[tag], errors='coerce')

    # Avoid division by zero by replacing zero denominators with NaN
    cols_to_replace_zero = ['Assets', 'Liabilities', 'StockholdersEquity'] 
    df_filtered.loc[:, cols_to_replace_zero] = df_filtered[cols_to_replace_zero].replace(0, np.nan)
    
    # Z'' Score Ratio Calculation (X1-X5)
    df_filtered['X1'] = (df_filtered['AssetsCurrent'] - df_filtered['LiabilitiesCurrent']) / df_filtered['Assets']
    df_filtered['X2'] = df_filtered['RetainedEarnings'] / df_filtered['Assets']
    df_filtered['X3'] = df_filtered['EBIT'] / df_filtered['Assets']
    df_filtered['X4'] = df_filtered['StockholdersEquity'] / df_filtered['Liabilities'] 
    
    # X5 (Revenues/Assets)는 Z'' Score 공식에 사용되지는 않지만, 데이터가 있다면 계산합니다.
    df_filtered['X5'] = np.nan
    has_revenues = df_filtered['Revenues'].notna() & df_filtered['Assets'].notna()
    df_filtered.loc[has_revenues, 'X5'] = df_filtered['Revenues'] / df_filtered['Assets']
    
    # Final Z'' Score (Uses only X1-X4)
    df_filtered['z_score'] = (
        6.56 * df_filtered['X1'] + 
        3.26 * df_filtered['X2'] + 
        6.72 * df_filtered['X3'] + 
        1.05 * df_filtered['X4']
    )
    
    # Valuation Ratio Calculation (ROE, EPS) (이하 동일)
    df_filtered['roe'] = np.nan
    df_filtered['eps'] = np.nan
    
    # ROE (NetIncomeLoss / StockholdersEquity)
    has_roe_data = df_filtered['NetIncomeLoss'].notna() & df_filtered['StockholdersEquity'].notna() & (df_filtered['StockholdersEquity'] != 0)
    df_filtered.loc[has_roe_data, 'roe'] = df_filtered['NetIncomeLoss'] / df_filtered['StockholdersEquity']
    
    # EPS (NetIncomeLoss / CommonStockSharesOutstanding)
    has_eps_data = df_filtered['NetIncomeLoss'].notna() & df_filtered['CommonStockSharesOutstanding'].notna() & (df_filtered['CommonStockSharesOutstanding'] != 0)
    df_filtered.loc[has_eps_data, 'eps'] = df_filtered['NetIncomeLoss'] / df_filtered['CommonStockSharesOutstanding']


    # Rounding and Prediction (이하 동일)
    df_filtered['X1'] = df_filtered['X1'].round(4)
    df_filtered['X2'] = df_filtered['X2'].round(4)
    df_filtered['X3'] = df_filtered['X3'].round(4)
    df_filtered['X4'] = df_filtered['X4'].round(4)
    df_filtered['X5'] = df_filtered['X5'].round(4)
    df_filtered['z_score'] = df_filtered['z_score'].round(3) 
    df_filtered['roe'] = df_filtered['roe'].round(4)
    df_filtered['eps'] = df_filtered['eps'].round(2) 

    def get_prediction(z):
        """Classifies the firm based on the Z'' Score."""
        if pd.isna(z): return 'Incomplete/Invalid'
        elif z > 2.6: return 'Safe' 
        elif z > 1.1: return 'Grey' 
        else: return 'Distress'

    df_filtered['prediction'] = df_filtered['z_score'].apply(get_prediction)
    
    # Clean up NaN z_score to 0.0 for records that were invalid/incomplete
    df_filtered.loc[
        df_filtered['z_score'].isna() & (df_filtered['prediction'] == 'Incomplete/Invalid'), 
        'z_score'
    ] = 0.0
    
    return df_filtered


# ===============================================
# 5. Database Storage Functions
# ===============================================

def get_db_connection():
    """Establishes and returns a PostgreSQL database connection."""
    return psycopg2.connect(**DB_CONFIG)

def safe_to_insert_list(results: List[Dict[str, Any]]) -> List[Tuple]:
    """Converts a list of result dictionaries into a list of tuples for DB insertion."""
    data_to_insert = []
    
    def safe_numeric_to_db(value):
        """Converts NaN/None values to None (NULL in DB)."""
        if pd.isna(value) or value is None:
            return None
        return str(value) 

    for r in results:
        # Handle 'fy' safely
        fy_val = r.get('fy')
        safe_fy = int(fy_val) if not pd.isna(fy_val) and fy_val is not None else None
            
        row = (
            r.get('adsh'), r.get('cik'), r.get('name'), r.get('ticker'), r.get('period'),         
            r.get('fp'), safe_fy, safe_numeric_to_db(r.get('x1_wcta')),
            safe_numeric_to_db(r.get('x2_reta')), safe_numeric_to_db(r.get('x3_ebitta')),
            safe_numeric_to_db(r.get('x4_mvtl')), safe_numeric_to_db(r.get('x5_salesta')),
            safe_numeric_to_db(r.get('net_income')), safe_numeric_to_db(r.get('total_equity')),
            safe_numeric_to_db(r.get('shares_outstanding')), r.get('z_score'), r.get('prediction'),
            safe_numeric_to_db(r.get('eps')), safe_numeric_to_db(r.get('roe')), r.get('data_source')    
        )
        data_to_insert.append(row)
        
    return data_to_insert

def save_results_to_db(results: List[Dict[str, Any]]):
    """Executes the bulk INSERT/UPDATE operation into the PostgreSQL database."""
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
    data_to_insert = safe_to_insert_list(results)
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.executemany(insert_query, data_to_insert)
        conn.commit()
        print(f"INFO: Successfully saved {len(data_to_insert)} analysis results to DB.")
    except (Exception, psycopg2.Error) as error:
        print(f"FATAL ERROR: Failed during DB saving. Details: {error}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()


# ===============================================
# 6. Main Execution Function (Bulk Processing) - FINAL FILTERING LOGIC
# ===============================================

def main_process(target_year: int = None):
    """Reads all files, calculates Z'' Score, and saves to the DB."""
    
    cik_to_ticker_map = load_cik_to_ticker_map()
    
    # Logic for selecting target directories based on target_year
    if target_year is not None:
        q_dir_y = os.path.join("data", f"{target_year}q4")
        q_dir_y_plus_1 = os.path.join("data", f"{target_year + 1}q1")
        
        quarter_dirs = []
        if os.path.isdir(q_dir_y):
            quarter_dirs.append(q_dir_y)
        if os.path.isdir(q_dir_y_plus_1):
            quarter_dirs.append(q_dir_y_plus_1)
            
        if quarter_dirs:
            print(f"INFO: Filtering directories for target year {target_year}: {quarter_dirs}")
        else:
            print(f"ERROR: Could not find specific directories for year {target_year}. Attempting full scan.")
            quarter_dirs = glob.glob(DATA_PATH_PATTERN)
    else:
        # Perform full scan if target_year is not specified.
        quarter_dirs = glob.glob(DATA_PATH_PATTERN) 
        print(f"INFO: Found {len(quarter_dirs)} quarter directories. Starting full scan.")
    
    if not quarter_dirs:
        print("ERROR: No quarter directories found in the specified path. Exiting.")
        return

    print(f"INFO: Starting data loading from {len(quarter_dirs)} directories...")
    
    all_combined_data = []
    for q_dir in quarter_dirs:
        df_combined = load_and_merge_data(q_dir, cik_to_ticker_map) 
        if not df_combined.empty:
            all_combined_data.append(df_combined)

    if not all_combined_data:
        print("WARNING: No data successfully loaded and combined across all directories. Exiting.")
        return

    df_final = pd.concat(all_combined_data, ignore_index=True)
    
    # Date format correction (YYYYMMDD to date object) - ROBUST VERSION
    try:
        df_final['period_float'] = pd.to_numeric(df_final['period'], errors='coerce')
        df_final.dropna(subset=['period_float'], inplace=True)
        df_final['period'] = df_final['period_float'].round(0).astype('Int64').astype(str)
        df_final.drop(columns=['period_float'], inplace=True)
        df_final = df_final[df_final['period'] != '<NA>'].copy()
        df_final = df_final[df_final['period'] != 'nan'].copy()
        df_final['period'] = pd.to_datetime(df_final['period'], format='%Y%m%d', errors='coerce').dt.date
    except Exception as e:
        print(f"WARNING: Could not fully convert 'period' column to date objects: {e}. Skipping conversion.")
    
    df_final.dropna(subset=['period', 'fy'], inplace=True) 
    
    df_to_analyze = df_final
    
    # 3. Apply year filtering (Dataframe level filtering - safety check)
    if target_year is not None:
        initial_count = len(df_to_analyze)
        
        # --- KEY FIX: Filter strictly on the Fiscal Year (fy) column ---
        df_to_analyze = df_to_analyze[df_to_analyze['fy'] == target_year].copy()
        # -------------------------------------------------------------
        
        if not df_to_analyze.empty:
            print(f"INFO: Final analysis set filtered to {target_year} reports ({len(df_to_analyze)} records). (Original: {initial_count})")
        else:
            print(f"WARNING: No Annual Reports found for the target year ({target_year}) after loading. Skipping analysis.")
            return

    # 4. Calculate Z-Score (Z'') and Valuation Ratios
    df_results = calculate_financial_ratios(df_to_analyze) 
    
    if df_results.empty: return

    print(f"INFO: Total calculated results ready for DB insertion: {len(df_results)}")

    # 5. Format and Save to DB
    df_results_subset = df_results.rename(columns={
        'X1': 'x1_wcta', 'X2': 'x2_reta', 'X3': 'x3_ebitta', 'X4': 'x4_mvtl', 'X5': 'x5_salesta',
        'NetIncomeLoss': 'net_income', 'StockholdersEquity': 'total_equity', 
        'CommonStockSharesOutstanding': 'shares_outstanding',
    })

    results_list = df_results_subset[
        ['adsh', 'cik', 'name', 'ticker', 'period', 'fp', 'fy',
         'x1_wcta', 'x2_reta', 'x3_ebitta', 'x4_mvtl', 'x5_salesta',
         'net_income', 'total_equity', 'shares_outstanding',
         'z_score', 'prediction', 'eps', 'roe', 'data_source']
    ].to_dict('records')

    if results_list:
        save_results_to_db(results_list)

# ===============================================
# 7. Diagnostic and Search Functions
# ===============================================
# Diagnostic functions must scan all directories to find the absolute latest report.

def find_company_data(cik_id: str):
    """Retrieves all analysis results for a specific CIK from the PostgreSQL database."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        search_query = """
        SELECT 
            name, ticker, period, fiscal_year, z_score, prediction, 
            x1_wcta, x2_reta, x3_ebitta, x4_mvtl, x5_salesta,
            eps, roe, data_source
        FROM financial_analysis_results
        WHERE cik = %s
        ORDER BY period DESC;
        """
        cur.execute(search_query, (cik_id,))
        columns = [desc[0] for desc in cur.description]
        results = cur.fetchall()
        
        if not results: return None
        
        df_results = pd.DataFrame(results, columns=columns)
        return df_results
        
    except (Exception, psycopg2.Error) as error:
        print(f"FATAL ERROR: Failed during DB search. Details: {error}")
        return None
    finally:
        if conn: conn.close()

def find_latest_annual_filing(cik_id: str) -> pd.DataFrame or None:
    """Finds the most recent Annual Report (FP='FY') for a specific CIK."""
    all_sub_data = []
    # Use global pattern to scan all quarters for the latest filing
    quarter_dirs = glob.glob(DATA_PATH_PATTERN)
    
    print(f"INFO: Starting scan for CIK {cik_id} file across {len(quarter_dirs)} quarter directories...")
    
    for q_dir in quarter_dirs:
        sub_path = os.path.join(q_dir, SUB_FILE_NAME)
        if os.path.exists(sub_path):
            try:
                df_sub = pd.read_csv(sub_path, sep='\t', usecols=SUB_COLUMNS_REQUIRED, 
                                     encoding='utf-8', on_bad_lines='skip', low_memory=False)
                df_sub['cik'] = df_sub['cik'].astype('Int64').astype(str)
                df_sub['data_source'] = q_dir
                all_sub_data.append(df_sub)
            except Exception: continue

    if not all_sub_data: return None

    df_combined_sub = pd.concat(all_sub_data, ignore_index=True)
    target_filings = df_combined_sub[
        (df_combined_sub['cik'] == cik_id) & 
        (df_combined_sub['fp'] == 'FY')
    ].copy()
    
    if target_filings.empty: return None
    
    # 2. Robust conversion for 'period' column (Fixing data type issues)
    try:
        target_filings['period_float'] = pd.to_numeric(target_filings['period'], errors='coerce')
        target_filings.dropna(subset=['period_float'], inplace=True)
        target_filings['period'] = target_filings['period_float'].round(0).astype('Int64').astype(str)
        target_filings.drop(columns=['period_float'], inplace=True)
        
        target_filings = target_filings[target_filings['period'] != '<NA>'].copy()
        target_filings = target_filings[target_filings['period'] != 'nan'].copy()
        
    except Exception as e:
        print(f"FATAL WARNING: Error during 'period' column type conversion. Details: {e}")
        return None
        
    # 3. Convert 'period' column to datetime objects
    target_filings['period_dt'] = pd.to_datetime(target_filings['period'], format='%Y%m%d', errors='coerce')
    
    # 4. Keep only rows with valid dates
    valid_filings = target_filings.dropna(subset=['period_dt']).copy()
    
    if valid_filings.empty: return None
        
    # 5. Find the latest filing based on the 'period_dt'
    latest_filing = valid_filings.loc[valid_filings['period_dt'].idxmax()]
    
    return latest_filing.to_frame().T

def diagnose_missing_tags_v2(cik_id: str):
    """Finds the latest Annual Report for a CIK, diagnoses missing core Z-Score tags, and suggests remedies."""
    
    latest_filing_df = find_latest_annual_filing(cik_id)
    
    if latest_filing_df is None or latest_filing_df.empty:
        print(f"\n--- CIK {cik_id} Diagnosis Result ---")
        print(f"FATAL: No valid Annual Report (FP='FY') found for CIK {cik_id}. Please check paths or data status.")
        return
        
    latest_filing = latest_filing_df.iloc[0]
    adsh, q_dir, fy, period, name = latest_filing['adsh'], latest_filing['data_source'], latest_filing['fy'], latest_filing['period'], latest_filing['name']
    num_path = os.path.join(q_dir, NUM_FILE_NAME)
    tag_rename_map = get_tag_rename_map(CANONICAL_TAG_MAP)

    print(f"\n--- CIK {cik_id} ({name}) Diagnosis Start ---")
    print(f"INFO: Latest Annual Report (FY: {fy}, Report Date: {period}) found in {os.path.basename(q_dir)} file.")
    
    # Load NUM file for diagnosis
    try:
        df_num = pd.read_csv(num_path, sep='\t', usecols=range(7), header=None, names=NUM_COLUMNS, encoding='utf-8', on_bad_lines='skip', low_memory=False)
    except Exception as e:
        print(f"ERROR: Error loading NUM file: {e}")
        return
        
    df_num_target = df_num[df_num['adsh'] == adsh].copy()
    df_num_target = df_num_target[df_num_target['dim'].isna()]
    df_num_target['canonical_tag'] = df_num_target['tag'].map(tag_rename_map)
    
    present_tags = df_num_target.dropna(subset=['canonical_tag'])['canonical_tag'].unique().tolist()
    missing_core_tags = [tag for tag in Z_SCORE_CORE_TAGS if tag not in present_tags]
    
    print(f"\n--- CIK {cik_id} Analysis Result ---")
    print(f"✅ Found Z'' Score Core Tags ({len(present_tags)}/{len(Z_SCORE_CORE_TAGS)}): {sorted(present_tags)}")
    
    if missing_core_tags:
        print(f"❌ Missing Z'' Score Core Tags: {missing_core_tags}")
        
        for missing_tag in missing_core_tags:
            all_original_tags = df_num_target['tag'].unique()
            
            # Find similar unmapped tags in the company's filing
            unmapped_tags_in_company = [
                tag for tag in all_original_tags 
                if (missing_tag.lower() in tag.lower() or missing_tag[:3].lower() in tag.lower()) 
                and tag not in tag_rename_map
            ]

            if len(unmapped_tags_in_company) > 0:
                 print(f"  * {missing_tag} Missing Cause: Found unmapped original tags with similar names!")
                 print(f"    -> Original Tag Examples: {unmapped_tags_in_company[:3]}")
                 print(f"    -> Solution: These tags should be added to the '{missing_tag}' Canonical Tag Map.")
            else:
                print(f"  * {missing_tag} Missing Cause: The item itself may be missing from this specific report.")

    else:
        print(f"🎉 Success! All 8 core tags required for CIK {cik_id}'s latest report are mapped.")
        
    print("--------------------------------------------------")


# ===============================================
# 8. Main Execution Block
# ===============================================

if __name__ == "__main__":
    
    # 1. Bulk Data Load, Z-Score Calculation, and DB Save
    # Goal: Analyze and save only 2023 reports.
    TARGET_YEAR = 2023
    print(f"\n--- Data Pipeline Start: Analysis Year {TARGET_YEAR} ---")
    
    # To run the bulk analysis and save to DB, UNCOMMENT the line below:
    main_process(target_year=TARGET_YEAR) 
    
    print("WARNING: main_process is commented out. Uncomment to run and verify DB settings.")
    
    # 2. Diagnose Missing Tags for a Specific CIK
    CIK_TO_CHECK = '1035443'
    print(f"\nINFO: Starting diagnosis for CIK {CIK_TO_CHECK} (Alexandria Real Estate Equities Inc.).")
    diagnose_missing_tags_v2(CIK_TO_CHECK)

    # 3. Search Specific CIK Data from DB (Runnable after main_process() has been run)
    # df_company_data = find_company_data(CIK_TO_CHECK)
    # if df_company_data is not None:
    #     print("\n--- DB Search Results (Latest First) ---")
    #     print(df_company_data)