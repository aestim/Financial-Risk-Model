import csv
import requests
import time
import os

# --- 1. Configuration ---
# REQUIRED: Change this to your 'Name/Company Name' and 'Email Address' to comply with SEC rate limits.
USER_AGENT = "tkfpsk@gmail.com"
HEADERS = {'User-Agent': USER_AGENT}

# File Paths
INPUT_FILE = "sec_8k_index_2025_Q1.csv"
OUTPUT_FILE = "bankruptcy_labels_2025_Q1.csv" # 🚨 This file is used as the checkpoint

# Base URL for SEC Archives
BASE_URL = "https://www.sec.gov/Archives"
# Master Index Field Names
FIELD_NAMES = ['CIK', 'Form Type', 'Filing Date', 'File Path']

# 🚨 Delimiter Configuration: Separate delimiters for Input and Output
INPUT_DELIMITER = '|'  # SEC index files use Pipe (|)
OUTPUT_DELIMITER = ',' # Output files use Comma (,)


# --- 2. Helper Functions ---

def get_filing_text(file_path):
    """ Downloads the 8-K filing text using the file path """
    try:
        # Attempt 1: Add .txt extension
        target_url = f"{BASE_URL}/{file_path}.txt"
        time.sleep(0.1) # Respect SEC Rate Limit
        response = requests.get(target_url, headers=HEADERS, timeout=10)
        
        if response.status_code == 200:
            return response.text
        
        # Attempt 2: Try without .txt extension if 404
        elif response.status_code == 404:
            target_url_alt = f"{BASE_URL}/{file_path}"
            time.sleep(0.1) 
            response_alt = requests.get(target_url_alt, headers=HEADERS, timeout=10)
            
            if response_alt.status_code == 200:
                return response_alt.text
            
            return None
        
        else:
            print(f"⚠️ Download failed (Status: {response.status_code}): {target_url}")
            return None
            
    except Exception as e:
        print(f"❌ Error during download ({file_path}): {e}")
        return None

def is_bankruptcy_event(text):
    """ Checks the filing text for the Item 1.03 keyword. """
    if text is None:
        return False
    return "item 1.03" in text.lower()

def get_last_processed_path(output_file, delimiter=OUTPUT_DELIMITER):
    """
    🚨 Final Checkpoint Logic: Reads the output CSV file directly and manually returns the last valid File Path.
    """
    if not (os.path.exists(output_file) and os.path.getsize(output_file) > 0):
        return None

    try:
        # Read all lines from the file.
        with open(output_file, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip()] 
        
        # Filter out the header line (by checking if it starts with the first field name 'CIK') and extract data rows.
        data_lines = [line for line in lines if not line.startswith(FIELD_NAMES[0])]
        
        if not data_lines:
            return None # No valid data found
            
        last_line = data_lines[-1]
        
        # Manually parse the last line using the Output Delimiter (Comma).
        parts = last_line.split(delimiter)
        path_index = 3 # File Path is the 4th field (index 3)
        
        if len(parts) > path_index:
            last_valid_path = parts[path_index].strip()
            
            if last_valid_path.startswith('edgar/data/'):
                print(f"DEBUG Result (Manual CSV Parsing): Last valid File Path is {last_valid_path}.")
                return last_valid_path
                
    except Exception as e:
        print(f"Warning: Manual checkpoint file reading error occurred. Error: {e}")
        return None
        
    return None


# --- 3. Main Function ---

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file '{INPUT_FILE}' not found.")
        return

    # 🚨 1. Check for checkpoint (Referencing OUTPUT_FILE)
    last_processed_path = get_last_processed_path(OUTPUT_FILE, delimiter=OUTPUT_DELIMITER)
    found_checkpoint = False
    
    if last_processed_path:
        print(f"📝 Checkpoint found: Last processed path is '{last_processed_path}'. (From: {OUTPUT_FILE})")
    else:
        print("📝 No checkpoint found. Starting processing from the beginning.")

    print(f"🔄 Starting analysis of '{INPUT_FILE}'...")
    
    bankruptcy_count = 0
    total_processed_skipped = 0 

    with open(INPUT_FILE, 'r', encoding='utf-8') as infile, \
         open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as outfile:
        
        # Input file DictReader configuration: Uses Pipe (|) delimiter
        reader = csv.DictReader(infile, delimiter=INPUT_DELIMITER, fieldnames=FIELD_NAMES)
        infile.seek(0)
        reader = csv.DictReader(infile, delimiter=INPUT_DELIMITER, fieldnames=FIELD_NAMES)
        try:
            next(reader) # Skip header row
        except StopIteration:
            print("Input file contains no data.")
            return

        # Output file DictWriter configuration: Uses Comma (,) delimiter
        is_new_file = os.path.getsize(OUTPUT_FILE) == 0
        writer = csv.DictWriter(outfile, fieldnames=FIELD_NAMES + ['Label'], delimiter=OUTPUT_DELIMITER)
        if is_new_file:
            writer.writeheader()

        for row in reader:
            # Prevent NoneType errors and validate data integrity
            if row.get('File Path') is None or row.get('CIK') == FIELD_NAMES[0]:
                continue 

            cik = row['CIK'].strip()
            file_path = row['File Path'].strip()
            
            if not file_path.startswith('edgar/data/'):
                 continue
            
            # 🚨 2. Resume Logic
            if last_processed_path and not found_checkpoint:
                total_processed_skipped += 1
                if file_path == last_processed_path:
                    found_checkpoint = True
                    print(f"✅ Checkpoint found: {last_processed_path}. Resuming processing from the next record.")
                continue 
            
            # --- 3. Normal Processing Starts ---
            total_processed_skipped += 1
            
            if total_processed_skipped % 100 == 0:
                print(f"... Currently processing {total_processed_skipped} filings (CIK: {cik})")

            filing_text = get_filing_text(file_path)
            
            if is_bankruptcy_event(filing_text):
                print(f"🚨 Bankruptcy filing found! CIK: {cik}, Date: {row['Filing Date'].strip()}")
                
                row['Label'] = 1
                writer.writerow(row)
                
                # Force save to the CSV result file (prevent data loss)
                outfile.flush()
                os.fsync(outfile.fileno()) 
                
                bankruptcy_count += 1

    print("-" * 50)
    print(f"✅ Analysis complete.")
    print(f"Total filings processed (including skipped): {total_processed_skipped} records")
    print(f"Bankruptcy filings found in this session: {bankruptcy_count} records")
    print(f"Results saved to '{OUTPUT_FILE}'.")

if __name__ == "__main__":
    main()