import pandas as pd
from pathlib import Path
from sec_cik_mapper import StockMapper 
import requests 
import csv # Import the csv module for quoting options

# Output file path for the final CIK mapping DataFrame
OUTPUT_FILE_PATH = 'data/processed/ndx_cik_mapping.csv'
WIKIPEDIA_URL = 'https://en.wikipedia.org/wiki/Nasdaq-100'

# Define a standard User-Agent header to mimic a web browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def fetch_tickers_from_wikipedia():
    """
    Fetches the current NASDAQ 100 Ticker list by explicitly fetching HTML content 
    and then parsing the tables to ensure stability against 403 errors and table index changes.
    
    Returns:
        pd.DataFrame: DataFrame with 'NDX_Name' and 'Ticker' columns, or None on failure.
    """
    print(f"Step 1: Fetching current NASDAQ 100 Ticker list from Wikipedia URL: {WIKIPEDIA_URL}")
    
    try:
        # 1. Use requests to fetch the HTML content with custom headers
        response = requests.get(WIKIPEDIA_URL, headers=HEADERS)
        response.raise_for_status() # Raise an HTTPError if the status is 4xx or 5xx

        # 2. Read all HTML tables from the fetched content
        tables = pd.read_html(response.text, header=0)
        
        target_df = None
        
        # 3. Iterate through tables to find the one containing the components
        for df in tables:
            # Check for the expected columns and row count
            if ('Ticker' in df.columns or 'Symbol' in df.columns) and len(df) > 90: 
                target_df = df
                break
        
        if target_df is None:
            raise ValueError("Could not locate the NASDAQ 100 components table based on column names and row count.")

        # --- Column Mapping ---
        if 'Ticker' in target_df.columns:
            ticker_col = 'Ticker'
        else: 
            ticker_col = 'Symbol'
            
        if 'Company' in target_df.columns:
            name_col = 'Company'
        elif 'Name' in target_df.columns:
            name_col = 'Name'
        else:
            name_col = target_df.columns[0]
            
        # Select and rename columns to standardize the DataFrame structure
        final_df = target_df.rename(columns={name_col: 'NDX_Name', ticker_col: 'Ticker'})
        
        # Keep only the required columns, drop rows where Ticker is missing, and remove duplicates
        final_df = final_df[['NDX_Name', 'Ticker']].dropna(subset=['Ticker']).drop_duplicates()
        
        return final_df.reset_index(drop=True)
        
    except requests.exceptions.RequestException as req_e:
        print(f"Error during HTTP request (e.g., connection, timeout): {req_e}")
        return None
    except ValueError as val_e:
        print(f"Error during HTML parsing or table selection: {val_e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

def map_company_to_cik_with_library_auto():
    """
    Fetches Tickers from Wikipedia robustly and maps them to CIK using sec-cik-mapper.
    """
    
    # 1. Ticker list load via Wikipedia scraping
    ndx_df = fetch_tickers_from_wikipedia()
    
    if ndx_df is None or ndx_df.empty:
        print("Fatal Error: Could not retrieve a valid Ticker list.")
        return None

    # --- Name Cleaning ---
    print(f"Successfully loaded {len(ndx_df)} fresh tickers.")
    print("Applying name cleaning: stripping surrounding quotes.")
    # Remove surrounding double quotes or single quotes from the company name
    ndx_df['NDX_Name'] = ndx_df['NDX_Name'].str.strip('\"\'')
    
    # Debug print after loading and cleaning
    print("\n--- DEBUG: First 5 Tickers Loaded from Wikipedia ---")
    print(ndx_df.head())
    print("\nData Types:")
    print(ndx_df.dtypes)
    print("--------------------------------------\n")
    
    # 2. Initialize SEC StockMapper (Prepare CIK mapping)
    print("Step 2: Initializing SEC StockMapper...")
    try:
        mapper = StockMapper()
    except Exception as e:
        print(f"Error initializing StockMapper: {e}")
        return None
        
    ticker_to_cik_map = mapper.ticker_to_cik

    # 3. Map Tickers to CIKs
    print("Step 3: Mapping Tickers to CIKs...")
    
    # --- DEBUG: CIK Mapping Trace (First 5) ---
    print("\n--- DEBUG: CIK Mapping Trace (First 5) ---")
    for i, ticker in enumerate(ndx_df['Ticker'].head(5)):
        # Retrieve the CIK as provided by the mapper (assumed 10-digit string)
        final_cik = ticker_to_cik_map.get(ticker, None)
        print(f"Ticker: {ticker}")
        print(f"  -> Final CIK (From mapper): {final_cik}")
        if i < 4:
            print("---")
    print("-------------------------------------------\n")
    
    # Apply mapping to the entire DataFrame
    # Use the CIK as provided by the mapper, assuming it is already 10-digit padded.
    ndx_df['CIK'] = ndx_df['Ticker'].apply(
        lambda x: ticker_to_cik_map.get(x, None)
    )
    
    # 4. Filter and Finalize
    
    # Successfully matched results
    final_output = ndx_df.dropna(subset=['CIK'])
    final_output = final_output[['NDX_Name', 'Ticker', 'CIK']].sort_values(by='NDX_Name').reset_index(drop=True)
    
    # Mismatch results (where CIK is None)
    mismatch_output = ndx_df[ndx_df['CIK'].isnull()]
    mismatch_output = mismatch_output[['NDX_Name', 'Ticker']].reset_index(drop=True)

    print(f"\n===== CIK Mapping Summary (Auto Fetch) =====")
    print(f"Total Tickers attempted: {len(ndx_df)}")
    print(f"Successful CIK matches: {len(final_output)}")
    print(f"Mismatched Tickers: {len(mismatch_output)}")
    print("=========================================")

    # --- ADDED: Mismatch List Output ---
    if not mismatch_output.empty:
        print("\n*** MISMATHED TICKER LIST (No CIK Found) ***")
        print(mismatch_output.to_string(index=False)) 
        print("*******************************************\n")

    print("\n--- DEBUG: First 5 Successfully Mapped CIKs ---")
    print(final_output.head())
    print("\nData Types:")
    print(final_output.dtypes)
    print("--------------------------------------------------\n")

    # 5. Save results
    Path('data/processed').mkdir(parents=True, exist_ok=True)
    # >>> MODIFIED LINE: Changed quoting=csv.QUOTE_NONE to quoting=csv.QUOTE_MINIMAL
    final_output.to_csv(OUTPUT_FILE_PATH, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"Final Ticker-CIK DataFrame saved to: {OUTPUT_FILE_PATH}")
    
    return final_output

if __name__ == '__main__':
    map_company_to_cik_with_library_auto()