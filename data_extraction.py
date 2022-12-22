import pandas as pd
import sqlalchemy
import tabula
from sqlalchemy import inspect, text
from sqlalchemy.ext.declarative import declarative_base
from tabula import read_pdf

from database_utils import DatabaseConnector as DBC


class DataExtractor():
    def list_db_tables(self, engine):
        inspector = inspect(engine)
        schemas = inspector.get_schema_names()

        tables = []
        for schema in schemas:
            for table_name in inspector.get_table_names("public"):
                tables.append(table_name)
        # print(tables)
        return tables

    def read_table_data(self, table_name, engine):
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM " + table_name))
        return result, result.keys()

    def extract_rds_table(self, engine, table_name):
        pd.set_option('display.max_columns', 10)
        table_name = self.list_db_tables(engine)[1]
        column = self.read_table_data(table_name, engine)[1]
        df = pd.DataFrame(self.read_table_data(
            table_name, engine)[0], columns=column)
        df.set_index("index")
        return df

    def retrieve_pdf_data(self, link):
        list_of_data = tabula.read_pdf(link, pages="all", lattice=True)
        df = pd.DataFrame(
            columns=["card_number", "expiry_date", "date_payment_confirmed"])


        for indexed_df in list_of_data:
            if len(indexed_df.axes[1]) == 5:
                indexed_df.drop(columns = indexed_df.columns[0], inplace = True, axis = 1)
                df = pd.concat([df, indexed_df], axis=0)
            else:
                print(indexed_df)
                quit()
            
            
         

        return df
