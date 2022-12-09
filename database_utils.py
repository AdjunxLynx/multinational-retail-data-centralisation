import sqlalchemy as sqla
from sqlalchemy import text
import yaml
from yaml.loader import SafeLoader
class DatabaseConnector():
    def read_db_creds(self):
        with open('db_creds.yaml') as f:
            data = yaml.load(f, Loader=SafeLoader)
            return data
    def init_db_engine(self):
            creds = self.read_db_creds(self)
            url = ("postgresql+psycopg2://" + creds["RDS_USER"] + ":" + creds["RDS_PASSWORD"] + "@" + creds["RDS_HOST"] + ":5432/" + creds["RDS_DATABASE"])
            print(url)
            engine = sqla.create_engine(url)
            engine
            return engine
    
