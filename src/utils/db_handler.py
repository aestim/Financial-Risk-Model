# comments are always written in English.
from sqlalchemy import create_engine

class DBHandler:
    def __init__(self, config):
        self.db_cfg = config.get('db', {})
        self.db_url = (
            f"postgresql://{self.db_cfg.get('user')}:"
            f"{self.db_cfg.get('password')}@"
            f"{self.db_cfg.get('host')}:"
            f"{self.db_cfg.get('port')}/"
            f"{self.db_cfg.get('database')}"
        )
        self.engine = create_engine(self.db_url)

    def get_engine(self):
        return self.engine