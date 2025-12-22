# comments are always written in English.
import json
from src.utils.db_handler import DBHandler
from src.core.tag_processor import TagProcessor
from src.core.cik_matcher import CIKMatcher

def main():
    # 1. Read the master config.json
    with open('config.json', 'r') as f:
        config = json.load(f)

    # 2. Initialize DBHandler with config
    db = DBHandler(config)
    engine = db.get_engine()

    # 3. Run CIK Matcher (Wikipedia -> CSV)
    # This part doesn't need DB for now as per your code
    # matcher = CIKMatcher(config)
    # matcher.map_and_save()

    # 4. Run Tag Processor (Files -> DB)
    # We pass BOTH config (for paths) and engine (for DB access)
    tag_processor = TagProcessor(config, engine)
    tag_processor.execute_full_process()

if __name__ == "__main__":
    main()