import pandas as pd
from pathlib import Path
import os

# --- Constants and File Paths ---
RAW_DATA_DIR = Path('data/raw')

# Target list of CIKs (Update this to point to your latest NDX CIK file)
NDX_CIK_MAPPING_PATH = Path('data/processed/ndx_cik_mapping.csv') 

# List of quarterly sub.txt files needed for 2024 Annual Report coverage
QUARTERLY_SUB_FILES = [
    # Updated paths to reflect the user's directory structure
    RAW_DATA_DIR / '2024q3' / 'sub.txt',
    RAW_DATA_DIR / '2024q4' / 'sub.txt',
    RAW_DATA_DIR / '2025q1' / 'sub.txt',
    RAW_DATA_DIR / '2025q2' / 'sub.txt',
]

# --- Verification Function ---
def verify_2024_10k_filings_comprehensive():
    """
    Verifies 2024 10-K/20-F filings by checking data across Q3 2024, Q4 2024, and Q1 2025 sub.txt files.
    """
    print("--- Starting Comprehensive 2024 Annual Report (10-K) Verification ---")
    
    # 1. Load the target CIKs from the NDX mapping file
    try:
        if not os.path.exists(NDX_CIK_MAPPING_PATH):
            raise FileNotFoundError(f"NDX CIK mapping file not found at {NDX_CIK_MAPPING_PATH}")
            
        ndx_mapping = pd.read_csv(NDX_CIK_MAPPING_PATH)
        required_ciks = set(ndx_mapping['CIK'].astype(str).str.zfill(10).unique())
        print(f"Loaded {len(required_ciks)} unique CIKs from the NDX list.")
    
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return

    # 2. Load and Aggregate Filing Data from Multiple Quarterly sub.txt files
    all_filed_data = []
    
    for file_path in QUARTERLY_SUB_FILES:
        try:
            if not os.path.exists(file_path):
                 print(f"Skipping missing file: {file_path}. Please ensure all files are downloaded.")
                 continue
                 
            print(f"Loading records from {file_path.relative_to(RAW_DATA_DIR)}...")
            
            sub_data = pd.read_csv(
                file_path, 
                sep='\t', 
                encoding='latin1', 
                usecols=['cik', 'form'],
                dtype={'cik': str} 
            )
            all_filed_data.append(sub_data)
            
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            continue

    if not all_filed_data:
        print("Fatal Error: No sub.txt files were successfully loaded. Cannot proceed with verification.")
        return

    # Combine data from all loaded quarters
    combined_sub_data = pd.concat(all_filed_data, ignore_index=True)
    print(f"\nTotal records loaded across all quarters: {len(combined_sub_data)}")

    # 3. Filter for Annual Reports (10-K and 20-F)
    # Filter for CIKs that are in our NDX list
    filed_10k_data = combined_sub_data[
        (combined_sub_data['form'].str.contains('10-K', na=False)) | 
        (combined_sub_data['form'].str.contains('20-F', na=False))
    ]
    
    # Process CIKs and get the unique set of filers
    filed_10k_data.loc[:, 'cik'] = filed_10k_data['cik'].str.zfill(10)
    filed_ciks = set(filed_10k_data['cik'].unique())

    # 4. Find Missing CIKs
    missing_ciks = required_ciks - filed_ciks
    
    # 5. Report Results
    print("\n--- Comprehensive Verification Results ---")
    
    if not missing_ciks:
        print("SUCCESS: All 10-K/20-F filings for the NDX list were found across Q3/Q4 2024 and Q1 2025 data!")
    else:
        print(f"WARNING: {len(missing_ciks)} NDX CIKs are still missing their 10-K/20-F filing.")
        
        # Get the names and tickers of missing companies
        missing_df = ndx_mapping[ndx_mapping['CIK'].astype(str).str.zfill(10).isin(missing_ciks)]
        print("\nRemaining Missing Companies List:")
        print(missing_df[['NDX_Name', 'Ticker', 'CIK']].to_string(index=False))

    print("------------------------------------------")


if __name__ == '__main__':
    # NOTE: Before running, ensure you have downloaded and placed 'sub.txt' in the 
    # data/raw/2024q3, data/raw/2024q4, and data/raw/2025q1 directories.
    verify_2024_10k_filings_comprehensive()