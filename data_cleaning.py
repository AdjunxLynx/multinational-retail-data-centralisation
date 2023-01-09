from database_utils import DatabaseConnector 
from data_extraction import DataExtractor
import pandas as pd
import re
import datetime
import numpy as np
import sqlalchemy as sqla
from pandasgui import show
from datetime import date


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
    
    def clean_provider(self, card):
        cards = []
        cards.append("Discover")
        cards.append("VISA 13 digit")
        cards.append("JCB 16 digit")
        cards.append("American Express")
        cards.append("Maestro")
        cards.append("Mastercard")
        cards.append("Diners Club / Carte Blanche")
        cards.append("VISA 19 digit")
        cards.append("JCB 15 digit")
        cards.append("VISA 16 digit")
        if card not in cards:
            card = np.NaN
        return card
    
    
    def clean_expiry(self, expiry):
        try:
            d = pd.to_datetime(expiry, errors = "coerce", format = "%m/%y")
            d = date(d.year, d.month, d.day)
        except Exception as E:
            d = np.NaN
            return d
            
            
        return d
    def clean_card_number(self, number):
        try:
            number = int(number)
            return int(number)
        except:
            return np.NaN
    
    def clean_continent(self, cont):
        continents = ["Europe", "America"]
        for continent in continents:
            if continent in cont:
                return continent
        return np.NaN
            
    def clean_country_code(self, code):
        if len(code) != 2:
            return np.NaN
        return code
    
    def clean_lat(self, lat):
        try:
            float(lat)
            return(lat)
        except:
            return np.NaN
    
    def clean_type(self, store_type):
        types = ["Local", "Outlet", "Super Store", "Mall Kiosk", "Web Portal"]
        for type in types:
            if type in store_type:
                return type
        return np.NaN
    
    def clean_store_code(self, code):
        try:
            code = str(code)
            letters, numbers = code.split("-")
            print(letters)
            print(numbers)
            print("#####")
        except Exception as E:
            print(E)
        pass
    
    def clean_int(self, number):
        try:
            int(number)
            return number
        except:
            #print(number, "from clean int")
            return np.NaN
        
    def clean_str(self, test_str):
        if any(chr.isdigit() for chr in test_str):
            #print(test_str, " from clean_str")
            return np.NaN
        return test_str
    
    def clean_float(self, test_str):
        try:
            test_str = float(test_str)
            return(test_str)
        except:
            #print(test_str, "from clean float")
            return np.NaN
            
    
    
    
    def called_clean_store_data(self, df):
        continent = []
        code = []
        latitude = []
        type = []
        date = []
        number = []
        locality = []
        longitude = []
        df = df.drop(axis = 0, columns = "lat")
        show(df)
        for index, rows in df.iterrows():
            continent.append(self.clean_continent(rows["continent"]))
            code.append(self.clean_country_code(rows["country_code"]))
            latitude.append(self.clean_lat(rows["latitude"]))
            type.append(self.clean_type(rows["store_type"]))
            date.append(self.clean_date(rows["opening_date"]))
            number.append(self.clean_int(rows["staff_numbers"]))
            locality.append(self.clean_str(rows["locality"]))
            longitude.append(self.clean_float(rows["longitude"]))
        
        print(longitude)
        
        df["continent"] = continent
        df["country_code"] = code
        df["latitude"] = latitude
        df["store_type"] = type
        df["opening_date"] = date
        df["staff_numbers"] = number
        df["locality"] = locality
        df["longitude"] = longitude
        
        
        show(df)
        pass
    
    def clean_card_data(self, df):
        date = []
        number = []
        expiry = []
        card = []
        df = df.dropna(axis = 0)
        df.reset_index(drop = True, inplace = True)
        for index, row in df.iterrows():
            date.append(self.clean_date(row["date_payment_confirmed"]))
            number.append(self.clean_card_number(row["card_number"]))
            expiry.append(self.clean_expiry(row["expiry_date"]))
            card.append(self.clean_provider(row["card_provider"]))
        df["date_payment_confirmed"] = date
        df["card_number"] = number
        df["expiry_date"] = expiry
        df["card_provider"] = card
        
        
        return df
        
           
            
                
        


DC = DataClean()

DE = DataExtractor()
DE.extract_from_s3(1)
quit()
endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
head_dict = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
stores = DE.list_number_of_stores(endpoint, head_dict)
endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{"
df = DE.retrieve_stores_data(endpoint, head_dict, stores)
DC.called_clean_store_data(df)



connector = DatabaseConnector()
engine = connector.init_db_engine()


url = ("postgresql+psycopg2://postgres:" + "Myacount1" + "@localhost:5432/Pagila")
local_engine = sqla.create_engine(url)
#connector.upload_to_db(DC.clean_user_data(engine, DE.list_db_tables(engine)[3]), local_engine, "dim_users")
#connector.upload_to_db(DC.clean_card_data(
    #DE.retrieve_pdf_data("card_details.pdf")), local_engine, "dim_card_details")
connector.upload_to_db(DC.called_clean_store_data(df),
                       local_engine, "dim_store_details")
