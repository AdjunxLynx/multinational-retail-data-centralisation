from database_utils import DatabaseConnector as DBC

import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
class DataExtractor():
    def list_db_tables(self):
        pd.set_option('display.max_rows', 1000)
        e = DBC()
        self.engine = e.init_db_engine()
        inspector = inspect(self.engine)
        schemas = inspector.get_schema_names()
        tables = []
        for schema in schemas:
            for table_name in inspector.get_table_names(schema=schema):
                for column in inspector.get_columns(table_name, schema=schema):
                    try:
                            str(int(column["name"]))
                    except:
                        tables.append(str(column["name"]))
        return tables
        
    def extract_rds_table(self, DBC, table):
        Base = declarative_base()
        with self.engine.connect() as connection:
            table_str = ""
            table = table
            for i in range(len(table)):
                table_str = ", ".join(table)
            print("here")
            print(table_str)
            print("here")
            result = connection.execute(f"SELECT * FROM {table_str}")
            column = result.keys()
            keys = column
            keys_joined = ""
            for i in keys:
                keys_joined.join(", " + str(i))
            query = f'SELECT {keys_joined} FROM {table}'
            
            result = connection.execute(query)
            info = []
            for element in result:
                info.append(element)
        column = sqlalchemy.engine.Result.keys()
        df = pd.DataFrame(data=info, columns=column)
db = DataExtractor()
e = DBC
db.extract_rds_table(e, db.list_db_tables())