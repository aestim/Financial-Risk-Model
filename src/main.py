# comments are always written in English.
import json
from src.utils.db_handler import DBHandler
from src.core.cik_matcher import CIKMatcher
from src.core.health_check import HealthCheck

def main():
    # 1. Load configuration
    with open('config.json', 'r') as f:
        config = json.load(f)

    # 2. Setup Database engine
    db = DBHandler(config)
    engine = db.get_engine()

    # 3. Step 1: Get Ticker/CIK mapping from Wikipedia
    matcher = CIKMatcher(config)
    ticker_df = matcher.map_and_save()

    if ticker_df is not None and not ticker_df.empty:
        # 4. Step 2: Conduct Financial Health Check (Altman Z''-Score)
        # Standardized tags are handled internally via edgartools
        checker = HealthCheck(config)
        result_df = checker.run_analysis(ticker_df)
        
        # 5. Step 3: Persistence
        # Store analysis results in the database
        result_df.to_sql('financial_health_reports', engine, if_exists='replace', index=False)
        
        # Save a physical copy in the processed data directory
        report_path = f"{config['paths']['processed_data_dir']}/health_report.csv"
        result_df.to_csv(report_path, index=False)
        
        print(f"[+] Process complete. Report saved to {report_path}")

if __name__ == "__main__":
    main()