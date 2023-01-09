import sqlalchemy as sqla
from sqlalchemy import text
import yaml
from yaml.loader import SafeLoader

class DatabaseConnector():
    def __init__(self) -> None:
         pass
     
    def read_db_creds(self):
        with open('db_creds.yaml') as f:
            data = yaml.load(f, Loader=SafeLoader)
            return data
    def init_db_engine(self):
            creds = self.read_db_creds()
            url = ("postgresql+psycopg2://" + creds["RDS_USER"] + ":" + creds["RDS_PASSWORD"] + "@" + creds["RDS_HOST"] + ":" + str(creds["RDS_PORT"]) + "/" + creds["RDS_DATABASE"])
            engine = sqla.create_engine(url)
            return engine
    
    def upload_to_db(self, df, engine, db_name):
        df.to_sql(db_name, engine, if_exists = "replace")
        with engine.connect() as conn:
    
            result = conn.execute(text("SELECT * FROM " + db_name))
            
                
                