import json
import os
import sys

# Add 'src' directory to sys.path to ensure absolute imports work correctly
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.cik_matcher import CIKMatcher
from core.filling_verifier import FilingVerifier

def load_config():
    """
    Load the centralized config.json from the project root.
    """
    # [English Comment] Get the absolute path of the project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, 'config.json')
    
    if not os.path.exists(config_path):
        print(f"[X] Error: Config file not found at {config_path}")
        sys.exit(1)
        
    with open(config_path, 'r') as f:
        return json.load(f)

def main():
    config = load_config()
    
    # Step 1: CIK Matching
    matcher = CIKMatcher(config)
    matcher.map_and_save()

    # Step 2: Filing Verification
    verifier = FilingVerifier(config)
    missing_companies = verifier.verify_filings()
    
    if not missing_companies.empty:
        print("[!] Missing Filers:")
        print(missing_companies.to_string(index=False))

    print("\n=== Pipeline Execution Finished ===")

if __name__ == "__main__":
    main()