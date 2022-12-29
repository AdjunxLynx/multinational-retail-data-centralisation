from database_utils import DatabaseConnector 
from data_extraction import DataExtractor
import pandas as pd
import re
import datetime
import numpy as np
import sqlalchemy as sqla
from pandasgui import show


class DataClean():
    def clean_user_data(self, engine, table = "legacy_users"):
        pd.set_option('display.max_columns', None)
        df = DataExtractor().extract_rds_table(engine, table)
        df.drop("index", axis=1)
        columns = df.columns.values.tolist()
        print(columns)
        
        print("Current Tables is: " + str(table))
        numbers = []
        dob = []
        join_date = []
        emails = []
        addresss = []
        
        for index, row in df.iterrows():

            #print("source:" + row["phone_number"])
            numbers.append(self.clean_number(row["phone_number"]))
            dob.append(self.clean_date(row["date_of_birth"]))
            join_date.append(self.clean_date(row["join_date"]))
            emails.append(self.clean_email(row["email_address"]))
            addresss.append(self.clean_address(row["address"]))
            
            
            
            
        pd.to_datetime(dob, format= "%Y/%m/%d", infer_datetime_format=True, errors = "coerce")
        pd.to_datetime(join_date, format= "%Y/%m/%d", infer_datetime_format=True, errors = "coerce")
        df["phone_number"] = numbers
        df["date_of_birth"] = dob
        df["email_address"] = emails
        df["address"] = addresss
        print("######################################")
        print(df["date_of_birth"].info())
        print("######################################")
        
        df["join_date"] = join_date
        return df
        
    
        
        
        
    def clean_number(self, number):
        try:
            number = number.replace("-", "")
        except:
            pass
        
        try:
            number = number.replace(".", "")
            number = number[:number.index("x")]
        except:
            pass
        
        try:
            number = number.replace(" ", "")
        except:
            pass
        
        try:
            temp = number[number.index("(")+1:number.index(")")]
            number = str(number[number.index(")")+1:])
            number = temp + number
        except Exception as E:
            pass
        
        try:
            number = number[number.index("+")+2:]
        except:
            pass
            
        try:
            if number[:2] == "00":
                number = number[1:]
        except:
            pass
        
        return number    
    def clean_date(self, date):
        
        month_text = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
        month_n0 =["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
        
        if date == "NULL":
            return np.nan
        try:
            pd.to_datetime(date, format = "%Y-%m-%d")
            return date
        except Exception as E:
            pass
            #print(date)
            
        try:
            true_date = ""
            values = date.split(" ")
            if len(values) == 1:
                return np.nan
            #print("values: " + str(date))
            
            for value in values:
                temp = re.sub("\D", "", value)
                if len(temp) == 4:
                    year = temp
                new_temp = re.sub(r'[0-9]+', '', value)
                if new_temp in month_text:
                    month = month_n0[month_text.index(new_temp)]
                if len(value) == 2:
                    day = value
            
            true_date = (year + "-" + month + "-" + day)
            #print(true_date)
            return true_date   
            
        except Exception as E:
            print(E)
            print(date)
            pass
            
        return date     
    def clean_email(self, email):
        if email == "NULL":
            return np.nan
        try:
            values = email.split("@")
            username = values[0]
            domain = values[1]
            return email
        except Exception as E:
            return np.nan
    def clean_address(self, address):
        if address == "NULL":
            return np.nan
            print("null")
        return address
    
    def clean_card_data(self, df):
        show(df)
           
            
                
        


DC = DataClean()

DE = DataExtractor()
DC.clean_card_data(DE.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"))
while True:
    pass


connector = DatabaseConnector()
engine = connector.init_db_engine()
url = ("postgresql+psycopg2://postgres:" + "Myacount1" + "@localhost:5432/Pagila")
local_engine = sqla.create_engine(url)
connector.upload_to_db(DC.clean_user_data(engine, DE.list_db_tables(engine)[3]), local_engine)
