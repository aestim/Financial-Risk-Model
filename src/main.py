# src/main.py
import os
from utils.cik_matcher import get_cik_list
# from core.tag_mapper import run_tag_mapper # Placeholder for next step
# from core.zscore_calculator import calculate_zscore # Placeholder for next step

# Define paths relative to the project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

NDX_FILE = os.path.join(PROJECT_ROOT, 'config', 'ndx_companies.csv')
SUB_FILE = os.path.join(PROJECT_ROOT, 'data', 'raw', 'sub.txt')
NUM_FILE = os.path.join(PROJECT_ROOT, 'data', 'raw', 'num.txt') 
TAG_FILE = os.path.join(PROJECT_ROOT, 'data', 'raw', 'tag.txt') 

def run_zscore_pipeline():
    """
    Executes the full Z-Score analysis pipeline.
    """
    print("--- Z-SCORE ANALYSIS PIPELINE STARTING ---")

    # 1. CIK Extraction & Filtering
    print("\nSTEP 1: Extracting CIKs from NDX list and SUB data...")
    target_ciks = get_cik_list(NDX_FILE, SUB_FILE)
    
    if not target_ciks:
        print("Pipeline failed: No CIKs found to analyze.")
        return

    print(f"STEP 1 Completed. Found {len(target_ciks)} CIKs for analysis.")
    # print(f"Target CIKs: {target_ciks}") # Uncomment to see the list

    # 2. Data Transformation (Tag Mapping & Value Extraction)
    # The target_ciks list is passed directly to the next stage.
    print("\nSTEP 2: Mapping XBRL tags to Z-Score variables (X1 to X5)...")
    # zscore_data_df = run_tag_mapper(target_ciks, NUM_FILE, TAG_FILE)
    
    # 3. Z-Score Calculation
    print("\nSTEP 3: Calculating Z-Scores and assessing distress...")
    # final_results = calculate_zscore(zscore_data_df)

    # 4. Reporting
    print("\nSTEP 4: Saving final results and generating visualizations...")
    # final_results.to_csv(os.path.join(PROJECT_ROOT, 'output', 'final_zscore_results.csv'))

    print("\n--- Z-SCORE ANALYSIS PIPELINE FINISHED ---")

if __name__ == '__main__':
    run_zscore_pipeline()