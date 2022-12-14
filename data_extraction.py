from database_utils import DatabaseConnector as DBC

import sqlalchemy
from sqlalchemy import inspect, text
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
class DataExtractor():
    def list_db_tables(self, engine):
        inspector = inspect(engine)
        schemas = inspector.get_schema_names()

        tables = []
        for schema in schemas:
            for table_name in inspector.get_table_names("public"):
                tables.append(table_name)
        print(tables)
        return tables
    
    def read_table_data(self, table_name, engine):
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM " + table_name))
        return result, result.keys()

    def extract_rds_table(self, engine, table_name):
        column = self.read_table_data(table_name, engine)[1]
        df = pd.DataFrame(self.read_table_data(table_name, engine)[0], columns = column)
        return df
        
        


