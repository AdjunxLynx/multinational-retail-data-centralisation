from database_utils import DatabaseConnector as DBC
from sqlalchemy import text
import pandas as pd
class DataExtractor():
    def list_db_tables(self):
        e = DBC
        self.engine = DBC.init_db_engine(e)
        with self.engine.connect() as connection:
            result = connection.execute(text("SELECT table_name"
                " FROM INFORMATION_SCHEMA.TABLES"
                " WHERE table_type = 'BASE TABLE'"))
            print(result)
            return result
    def extract_rds_table(self, table):
        table = self.list_db_tables()
        df = pd.DataFrame(data=tables)
        print(df)
    
    def loop(self):
        df = pd.DataFrame(data = [])
        tables = self.list_db_tables()
        for i in self.list_db_tables():
            df.append(self.extract_rds_table(table = tables[i]))
        
        return df
        
db = DataExtractor()
db.loop()