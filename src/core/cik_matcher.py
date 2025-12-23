import pandas as pd
import requests
import csv
import io
import os
from pathlib import Path
from sec_cik_mapper import StockMapper

class CIKMatcher:
    def __init__(self, config):
        """
        Initialize CIKMatcher with centralized configuration.
        """
        # Load sections from config
        self.matcher_cfg = config.get('cik_matcher', {})
        self.paths_cfg = config.get('paths', {})
        self.settings = config.get('settings', {})
        
        # Build absolute output path
        # Assuming execution from project root or handling via main.py
        self.output_path = os.path.join(
            self.paths_cfg.get('processed_data_dir', 'data/processed'),
            self.matcher_cfg.get('output_file_name', 'cik_mapping.csv')
        )
        
        # HTTP headers and Index URLs for expansion
        self.headers = self.matcher_cfg.get('headers', {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.index_urls = self.matcher_cfg.get('index_urls', {
            "NDX": "https://en.wikipedia.org/wiki/Nasdaq-100",
            "SP500": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        })

    def fetch_tickers_from_wikipedia(self, index_type):
        """
        Fetches ticker list from Wikipedia based on index_type (NDX, SP500, etc.)
        Handles table parsing robustly.
        """
        url = self.index_urls.get(index_type)
        if not url:
            print(f"[X] Error: Unsupported index type: {index_type}")
            return None

        print(f"[*] Fetching {index_type} ticker list from Wikipedia...")
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            # Using io.StringIO to avoid FutureWarning in pandas read_html
            html_io = io.StringIO(response.text)
            tables = pd.read_html(html_io, header=0)
            
            target_df = None
            # Find the component table (usually with 'ticker' or 'symbol' columns)
            for df in tables:
                cols = [c.lower() for c in df.columns]
                if any(k in cols for k in ['ticker', 'symbol']) and len(df) > 90:
                    target_df = df
                    break
            
            if target_df is None:
                raise ValueError(f"Could not locate the {index_type} components table.")

            # Standardize column names for different Wikipedia table structures
            target_df.columns = [c.lower() for c in target_df.columns]
            ticker_col = 'ticker' if 'ticker' in target_df.columns else 'symbol'
            
            # Map company name column (often 'company' or 'name' or the first column)
            if 'company' in target_df.columns:
                name_col = 'company'
            elif 'name' in target_df.columns:
                name_col = 'name'
            else:
                name_col = str(target_df.columns[0])
                
            final_df = target_df.rename(columns={name_col: 'company_name', ticker_col: 'ticker'})
            return final_df[['company_name', 'ticker']].dropna(subset=['ticker']).drop_duplicates()  # type: ignore
            
        except Exception as e:
            print(f"[X] Error during Wikipedia fetch: {e}")
            return None

    def map_and_save(self, index_type=None):
        """
        Main execution: Fetch -> Map to CIK -> Save to CSV
        """
        # Determine index type from argument or config
        target_index = index_type or self.settings.get('target_index', 'NDX')
        
        # 1. Fetch from Wikipedia
        df = self.fetch_tickers_from_wikipedia(target_index)
        if df is None or df.empty:
            print("[X] Fatal Error: No tickers found.")
            return None

        # 2. Name Cleaning
        df['company_name'] = df['company_name'].str.strip('\"\'')
        
        # 3. Map Tickers to CIK using sec-cik-mapper
        print("[*] Initializing StockMapper and mapping CIKs...")
        try:
            mapper = StockMapper()
            ticker_to_cik_map = mapper.ticker_to_cik
        except Exception as e:
            print(f"[X] Error initializing StockMapper: {e}")
            return None

        df['cik'] = df['ticker'].apply(lambda x: ticker_to_cik_map.get(x, None))
        
        # 4. Filter and Finalize
        final_output = df.dropna(subset=['cik']).sort_values(by='company_name').reset_index(drop=True)
        mismatch_count = len(df) - len(final_output)

        print(f"\n===== {target_index} CIK Mapping Summary =====")
        print(f"Total Tickers: {len(df)}")
        print(f"Mapped CIKs:   {len(final_output)}")
        print(f"Mismatches:    {mismatch_count}")
        print("=========================================")

        # 5. Save results using quoting=csv.QUOTE_MINIMAL as requested
        output_dir = Path(self.output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        final_output.to_csv(self.output_path, index=False, quoting=csv.QUOTE_MINIMAL)
        print(f"[+] Mapping saved to: {self.output_path}")
        
        return final_output