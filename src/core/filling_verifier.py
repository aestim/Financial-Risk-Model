import pandas as pd
from pathlib import Path
import os
from typing import List

class FilingVerifier:
    def __init__(self, config):
        """
        Initialize FilingVerifier with centralized configuration.
        """
        self.paths_cfg = config.get('paths', {})
        self.matcher_cfg = config.get('cik_matcher', {})
        self.settings = config.get('settings', {})

        # Set paths from config
        self.raw_data_dir = Path(self.paths_cfg.get('raw_data_dir', 'data/raw'))
        
        # Build the CIK mapping path dynamically
        self.cik_mapping_path = Path(os.path.join(
            self.paths_cfg.get('processed_data_dir', 'data/processed'),
            self.matcher_cfg.get('output_file_name', 'cik_mapping.csv')
        ))

    def _generate_quarterly_paths(self, target_year: str) -> List[Path]:
        """
        Generates quarterly sub.txt paths based on the target fiscal year.
        """
        try:
            current_year = int(target_year)
            next_year = current_year + 1 
        except ValueError:
            print(f"[X] Error: Invalid year format: {target_year}")
            return []
        
        # Quarters to search: Q3, Q4 of current year and Q1, Q2 of next year
        quarters = [
            f'{current_year}q3', f'{current_year}q4',
            f'{next_year}q1', f'{next_year}q2',
        ]
        
        return [self.raw_data_dir / q / 'sub.txt' for q in quarters]

    def verify_filings(self, target_fy=None):
        """
        Identifies companies that haven't filed their annual reports (10-K/20-F).
        """
        # Use target_fy from argument or config
        fy = target_fy or self.settings.get('target_fy', '2024')
        
        print(f"[*] Verifying annual report filings for FY {fy}...")

        # 1. Load CIK Mapping
        if not self.cik_mapping_path.exists():
            print(f"[X] Error: Mapping file not found at {self.cik_mapping_path}")
            return pd.DataFrame()

        ndx_mapping = pd.read_csv(self.cik_mapping_path)
        # Ensure CIK is treated as string for matching
        ndx_mapping['cik'] = ndx_mapping['cik'].astype(str)
        required_ciks = set(ndx_mapping['cik'].unique())

        # 2. Scan SEC raw data (sub.txt)
        quarterly_files = self._generate_quarterly_paths(fy)
        filed_ciks = set()

        for file_path in quarterly_files:
            if not file_path.exists():
                print(f"[!] Warning: Data file missing: {file_path}")
                continue
            
            # Load submission data, focusing on CIK and Form type
            sub_df = pd.read_csv(file_path, sep='\t', usecols=['cik', 'form'])
            sub_df['cik'] = sub_df['cik'].astype(str)
            
            # Filter for Annual Reports (10-K or 20-F)
            annual_reports = sub_df[sub_df['form'].isin(['10-K', '20-F'])]
            filed_ciks.update(annual_reports['cik'].unique())

        # 3. Identify missing filings
        ndx_filed_ciks = required_ciks.intersection(filed_ciks)
        missing_ciks = required_ciks - ndx_filed_ciks
        
        missing_df = ndx_mapping[ndx_mapping['cik'].isin(missing_ciks)].copy()
        
        print(f"\n===== Filing Verification Summary (FY {fy}) =====")
        print(f"Total Companies:  {len(required_ciks)}")
        print(f"Filed Reports:    {len(ndx_filed_ciks)}")
        print(f"Missing Reports:  {len(missing_ciks)}")
        print("================================================\n")
        
        return missing_df[['cik', 'company_name']].sort_values(by='company_name')