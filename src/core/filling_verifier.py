import pandas as pd
from pathlib import Path
import os
from typing import List
import argparse 

# --- Constants and File Paths ---
RAW_DATA_DIR = Path('data/raw') 
# Target list of CIKs (NDX CIK mapping file path, adjustment needed for the actual environment)
NDX_CIK_MAPPING_PATH = Path('data/processed/ndx_cik_mapping.csv') 

def _generate_quarterly_paths(target_year: str, raw_data_dir: Path) -> List[Path]:
    """
    Dynamically generates the list of quarterly sub.txt file paths 
    for the specified fiscal year (Q3, Q4) and the subsequent year (Q1, Q2).
    """
    try:
        current_year = int(target_year)
        # Calculate the year for Q1 and Q2 of the filing window
        next_year = current_year + 1 
    except ValueError:
        print(f"Error: Invalid year format provided: {target_year}. Please use a four-digit year string.")
        return []
    
    # Define the quarters to search relative to the target FY
    quarters = [
        f'{current_year}q3',
        f'{current_year}q4',
        f'{next_year}q1',
        f'{next_year}q2',
    ]
    
    # Construct the full Path objects for sub.txt in each quarter directory
    quarterly_sub_files = [raw_data_dir / q / 'sub.txt' for q in quarters]
    return quarterly_sub_files


def find_missing_ndx_2024_filings(target_fy: str) -> pd.DataFrame:
    """
    Finds companies in the NDX list that have not filed their 10-K/20-F 
    annual report with fy=target_fy within the loaded SEC data range.
    """
    
    # 1. Load NDX CIKs and Company Names
    try:
        if not os.path.exists(NDX_CIK_MAPPING_PATH):
            print(f"Error: NDX CIK mapping file not found at {NDX_CIK_MAPPING_PATH}")
            return pd.DataFrame()
            
        # --- MODIFICATION 1: Specify 'CIK' as string dtype during loading ---
        # This prevents pandas from inferring it as int64 initially, solving the FutureWarning.
        ndx_mapping = pd.read_csv(NDX_CIK_MAPPING_PATH, dtype={'CIK': str})
        # -------------------------------------------------------------------
        
        # Handle CIK format: ensure it is a 10-digit string
        # Explicit casting is still safe, but the warning should now be gone
        ndx_mapping.loc[:, 'CIK'] = ndx_mapping['CIK'].astype(str).str.zfill(10)
        
        required_ciks = set(ndx_mapping['CIK'].unique())
        print(f"Loaded {len(required_ciks)} unique CIKs from the NDX list.")
        
        # Determine the column to use for company name display
        if 'NDX_Name' not in ndx_mapping.columns:
            print("Warning: 'Name' column not found in NDX mapping file. Using first available string column for display.")
            name_col = next((col for col in ndx_mapping.columns if ndx_mapping[col].dtype == 'object'), None)
            if name_col is None:
                name_col = 'CIK'
        else:
            name_col = 'NDX_Name'
    
    except Exception as e:
        print(f"Error loading NDX CIK mapping file: {e}")
        return pd.DataFrame()

    # Dynamic path generation based on the target_fy
    QUARTERLY_SUB_FILES = _generate_quarterly_paths(target_fy, RAW_DATA_DIR)
    print(f"Searching filings in: {[p.parts[-2] for p in QUARTERLY_SUB_FILES]}")

    # 2. Find All Filed CIKs matching the criteria (fy=target_fy and 10-K/20-F)
    all_filed_data = []
    
    for file_path in QUARTERLY_SUB_FILES:
        try:
            if not os.path.exists(file_path):
                 continue
                 
            # Load only necessary columns
            sub_data = pd.read_csv(
                file_path, 
                sep='\t', 
                encoding='latin1', 
                usecols=['cik', 'form', 'fy'], 
                # 'cik' is already specified as str here from the previous version, which is correct.
                dtype={'cik': str, 'fy': str} 
            )
            all_filed_data.append(sub_data)
            
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            continue

    if not all_filed_data:
        print("Error: No sub.txt files were successfully loaded.")
        return pd.DataFrame()

    combined_sub_data = pd.concat(all_filed_data, ignore_index=True)
    
    # Ensure 'cik' is 10-digit string after concatenation
    # Explicit casting is safe and prevents potential future warnings here too.
    combined_sub_data.loc[:, 'cik'] = combined_sub_data['cik'].astype(str).str.zfill(10).astype(str)
    
    # Condition A: Annual report forms (10-K or 20-F)
    is_annual_report = (combined_sub_data['form'].str.contains('10-K', na=False)) | \
                       (combined_sub_data['form'].str.contains('20-F', na=False))

    # Condition B: Fiscal Year (fy) column matches the target_fy
    is_target_fy = combined_sub_data['fy'] == target_fy

    # Condition C: CIK is included in the NDX list (Filter NDX companies only)
    is_ndx_filer = combined_sub_data['cik'].isin(required_ciks)

    # Extract CIKs of NDX companies that filed an annual report with fy=target_fy
    ndx_filed_ciks_data = combined_sub_data[is_annual_report & is_target_fy & is_ndx_filer]
    ndx_filed_ciks = set(ndx_filed_ciks_data['cik'].unique())

    # 3. Identify Missing Filers 
    # Find CIKs present in the NDX list but NOT in the filed reports list
    missing_ciks = required_ciks - ndx_filed_ciks
    
    # Map the missing CIKs back to the company names
    missing_filers_df = ndx_mapping[ndx_mapping['CIK'].isin(missing_ciks)].copy()
    
    print(f"\nSUCCESS: Found {len(missing_ciks)} missing Annual Reports (FY={target_fy}).")
    
    # 4. Final Output
    return missing_filers_df[['CIK', name_col]].sort_values(by=name_col)


if __name__ == '__main__':
    # Initialize the argument parser for command-line execution
    parser = argparse.ArgumentParser(
        description="Find NDX companies missing their annual report (10-K/20-F) for a specific fiscal year."
    )
    
    # Add the argument for the target fiscal year (required)
    parser.add_argument(
        '--fy', 
        type=str, 
        required=True, 
        help="The target Fiscal Year (e.g., '2024') to check for missing 10-K/20-F filings."
    )
    
    # Parse the arguments from the command line
    args = parser.parse_args()
    TARGET_YEAR = args.fy 
    
    # Execute the core function with the dynamically obtained target year
    missing_companies_df = find_missing_ndx_2024_filings(TARGET_YEAR)
    
    if not missing_companies_df.empty:
        print(f"\n--- NDX Companies Missing Annual Report (FY={TARGET_YEAR}) ---")
        print(missing_companies_df.to_string(index=False))
    else:
        print(f"\nAll NDX companies in the list have filed their Annual Report (FY={TARGET_YEAR}) within the loaded data range.")