import json
import os
import sys

# Add 'src' directory to sys.path to ensure absolute imports work correctly
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.cik_matcher import CIKMatcher

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
    # 1. Load configuration
    config = load_config()
    settings = config.get('settings', {})
    
    print("=== Financial Risk Model Pipeline Started ===")

    # 2. CIK Matching Step
    # [English Comment] Initialize CIKMatcher with the injected config object
    matcher = CIKMatcher(config)
    
    # [English Comment] Execute mapping based on target_index defined in config
    target_index = settings.get('target_index', 'NDX')
    mapping_df = matcher.map_and_save(index_type=target_index)

    if mapping_df is not None:
        print(f"[+] Step 1 Complete: {len(mapping_df)} companies mapped to CIK.")
    else:
        print("[X] Step 1 Failed: Check internet connection or Wikipedia URL.")
        return

    # 3. Future Steps (Dictionary Building, Verification, Calculation)
    # print("\n[*] Moving to next stage: Dictionary Building...")
    # builder = DictionaryBuilder(config)
    # ...

    print("\n=== Pipeline Execution Finished ===")

if __name__ == "__main__":
    main()