import requests
import time
import csv

# --- Configuration (Adhering to SEC Recommendations) ---
# The SEC requires setting a User-Agent for automated access.
# MUST fill in your 'Company/Individual Name' and 'Email Address' for actual use.
USER_AGENT = "tkfpsk@gmail.com"
HEADERS = {'User-Agent': USER_AGENT}

# Set the target year and quarter for crawling.
# Quarter is 1: Jan-Mar, 2: Apr-Jun, 3: Jul-Sep, 4: Oct-Dec.
TARGET_YEAR = 2025
TARGET_QUARTER = 1

# Output file for storing filtered 8-K records
OUTPUT_DIR = "data/2025q1"
OUTPUT_FILE = f"{OUTPUT_DIR}/sec_8k_index_{TARGET_YEAR}_Q{TARGET_QUARTER}.csv"

# Master Index File Download URL
INDEX_URL = f"https://www.sec.gov/Archives/edgar/full-index/{TARGET_YEAR}/QTR{TARGET_QUARTER}/master.idx"

def download_and_filter_8k_index(year, qtr):
    """
    Downloads the SEC EDGAR Master Index file, filters for 8-K filings, and saves them.
    """
    print(f"[{year} Q{qtr}] Attempting to download the Master Index file...")
    
    try:
        # Respect SEC Rate Limit
        time.sleep(1) 
        
        response = requests.get(INDEX_URL, headers=HEADERS)
        response.raise_for_status() # Raise an exception for HTTP errors

        # The Master Index file is a plain text file with a special structure.
        # The actual data starts from the 11th line (index 10).
        index_content = response.text.split('\n')
        
        # Process and filter the file
        data_rows = []
        
        # Data starts from line 11 (index 10)
        for line in index_content[10:]:
            # SEC index files are pipe-separated
            parts = line.split('|')
            if len(parts) < 5:
                continue

            cik = parts[0]
            form_type = parts[2]
            filing_date = parts[3]
            # The last part contains the file path (Accession Number)
            accession_number = parts[4].replace('.txt', '') 
            
            # Filter only for '8-K' Form Type reports
            if form_type == '8-K':
                # The file path used for later download is constructed from the accession number
                file_path = f"edgar/data/{cik}/{accession_number.replace('-', '')}/{accession_number}.txt"
                data_rows.append([cik, form_type, filing_date, file_path])

        # Save to a CSV file
        # IMPORTANT: This index file uses '|' as a delimiter, maintaining this format.
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='|')
            writer.writerow(['CIK', 'Form Type', 'Filing Date', 'File Path'])
            writer.writerows(data_rows)

        print(f"✅ {len(data_rows)} 8-K filing records saved to '{OUTPUT_FILE}'.")
        print("Next step: Use the 'File Path' column to download individual 8-K filings for analysis.")

    except requests.exceptions.RequestException as e:
        print(f"❌ Download error occurred: {e}")
        print("Check your User-Agent, as access may have been denied by the SEC or the URL might be incorrect.")

if __name__ == "__main__":
    download_and_filter_8k_index(TARGET_YEAR, TARGET_QUARTER)