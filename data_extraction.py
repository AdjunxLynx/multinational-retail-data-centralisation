import pandas as pd
import sqlalchemy
import tabula
from sqlalchemy import inspect, text
from sqlalchemy.ext.declarative import declarative_base
from tabula import read_pdf
from pandasgui import show
import numpy as np
import time
import requests
import ast
from database_utils import DatabaseConnector as DBC
import datetime
import boto3
import botocore



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

    def list_number_of_stores(self, store_endpoints, head_dict):
        response = requests.get(
            store_endpoints, headers=head_dict)
        byte = response.content
        response_dict = byte.decode("UTF-8")
        response_dict = ast.literal_eval(response_dict)
        stores = response_dict["number_stores"]
        print(stores, "stores")
        return (stores)

    def retrieve_stores_data(self, store_endpoint, head_dict, stores):
        stores_df = pd.DataFrame(columns=["index", "address", "longitude", "lat", "locality", "store_code",
                                 "staff_numbers", "opening_date", "store_type", "latitude", "country_code", "continent"])
        store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
        null = "null"
        before = time.time()
        for i in range(stores):
            response = requests.get(store_endpoint + str(i), headers=head_dict)
            byte = response.content
            response_dict = byte.decode("UTF-8")
            response_dict = eval(response_dict)
            temp = pd.DataFrame(response_dict, index = [i])
            stores_df = pd.concat([stores_df, temp])
        print(time.time() - before, " seconds passed")
        stores_df.drop("index", axis = "columns", inplace = True)
        return(stores_df)

    def extract_from_s3(self, address):
        s3 = boto3.client("s3")
        data = s3.download_file("data-handling-public",
                                "products.csv", "product_list.csv")
                
        

    def retrieve_pdf_data(self, link):
        list_of_data = tabula.read_pdf(
            link, pages="all", lattice=True, guess=True)
        clean_df = pd.DataFrame(columns=[
                                "index", "card_number", "expiry_date", "card_provider", "date_payment_confirmed"])
        list_of_data[0].columns = ["index", "card_number",
                                   "expiry_date", "card_provider", "date_payment_confirmed"]
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
            if len(temp) == 0:
                temp = name
            temp_df = pd.DataFrame([temp], columns=name)

            print(df.columns)
            print(temp)

            temp_df = pd.DataFrame([temp], columns = name)
    
            for col in df.columns:
                if df[col].isnull().values.all():
                    df.drop(axis=1, columns=col, inplace=True)

            df.columns = ["index", "card_number", "expiry_date",
                          "card_provider", "date_payment_confirmed"]

            clean_df = pd.concat([clean_df, temp_df], axis=0)
            # clean_df.iloc[-1] = temp
            clean_df = pd.concat([clean_df, df], axis=0)

        clean_df = clean_df.iloc[2:, :]
        clean_df = clean_df.set_index("index")
        print(str((time.time() - before)) + "seconds taken")
        return clean_df
