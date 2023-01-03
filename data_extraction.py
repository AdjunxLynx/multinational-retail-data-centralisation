import pandas as pd
import sqlalchemy
import tabula
from sqlalchemy import inspect, text
from sqlalchemy.ext.declarative import declarative_base
from tabula import read_pdf
from pandasgui import show
import numpy as np
import time

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
        list_of_data = tabula.read_pdf(link, pages="all", lattice=True, guess = True)
        clean_df = pd.DataFrame(columns = ["index", "card_number", "expiry_date", "card_provider", "date_payment_confirmed"])
        list_of_data[0].columns = ["index", "card_number", "expiry_date", "card_provider", "date_payment_confirmed"]
        clean_df = pd.concat([clean_df, list_of_data[0]])
        before = time.time()
        for i in range(1, len(list_of_data)):
            df = list_of_data[i]
            temp = []
            name = ["index", "card_number", "expiry_date",
                    "card_provider", "date_payment_confirmed"]
            for col in df.columns:
                if "Unnamed:" in col:
                    pass
                else:
                    temp.append(col)
<<<<<<< HEAD
            temp = pd.DataFrame(temp)
=======
            if len(temp) == 0:
                temp = name
            print(df.columns)
            print(temp)

            temp_df = pd.DataFrame([temp], columns = name)
    
>>>>>>> parent of 1f7a7c1... ex
            for col in df.columns:
                if df[col].isnull().values.all():
                    df.drop(axis = 1, columns = col, inplace = True)
            
            df.columns = ["index", "card_number", "expiry_date", "card_provider", "date_payment_confirmed"]
            clean_df = pd.concat([clean_df, temp], axis=0)
            clean_df = pd.concat([clean_df, df], axis = 0)
            #print(clean_df.head())
        
        clean_df = clean_df.iloc[2:, :]
        clean_df.drop(axis=1, columns=clean_df.columns[-1], inplace = True)
        clean_df = clean_df.set_index("index")
        print(str((time.time() - before)) + "seconds taken")
        return clean_df
