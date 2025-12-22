import os
import pandas as pd
from sqlalchemy import text

class TagProcessor:
    def __init__(self, config, engine):
        """
        Initialize TagProcessor with configuration and database engine.
        """
        self.paths_cfg = config.get('paths', {})
        self.raw_path = self.paths_cfg.get('raw_data_dir', 'data/raw')
        self.engine = engine

    def execute_full_process(self):
        """
        Run the full pipeline: Ingest raw tag files and perform deduplication.
        """
        self.ingest_raw_tags()
        self.remove_duplicates()

    def ingest_raw_tags(self):
        """
        Read all tag.txt files from the raw data directory and load them into the DB.
        """
        print(f"[*] Ingesting tags from: {self.raw_path}")
        
        is_first_file = True
        folders = sorted(os.listdir(self.raw_path))
        
        for folder_name in folders:
            folder_path = os.path.join(self.raw_path, folder_name)
            file_path = os.path.join(folder_path, "tag.txt")
            
            if os.path.isdir(folder_path) and os.path.exists(file_path):
                print(f" [>] Processing: {folder_name}")
                try:
                    # Load tags without adding source_folder to maintain a clean dictionary
                    df = pd.read_csv(file_path, sep='\t', encoding='ISO-8859-1')
                    
                    # Use 'replace' for the first file to reset the schema and clear previous junk data
                    mode = 'replace' if is_first_file else 'append'
                    df.to_sql('raw_tags', self.engine, if_exists=mode, index=False, chunksize=10000)
                    is_first_file = False
                except Exception as e:
                    print(f" [X] Error processing {folder_name}: {e}")

    def remove_duplicates(self):
        """
        Remove duplicate tags globally.
        Only 'tag' is used for grouping to handle inconsistent 'version' formats in 2025+ data.
        """
        print("[*] Deduplicating by 'tag' only...")
        
        # Keep only the first occurrence (MIN ctid) for each unique tag name
        query = text("""
            DELETE FROM raw_tags
            WHERE ctid NOT IN (
                SELECT MIN(ctid) 
                FROM raw_tags 
                GROUP BY tag
            );
        """)
        
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()
        
        # Log the final unique tag count for verification
        check_query = text("SELECT COUNT(*) FROM raw_tags;")
        with self.engine.connect() as conn:
            final_count = conn.execute(check_query).scalar()
            print(f"[V] Clean-up finished. Total unique tags: {final_count}")