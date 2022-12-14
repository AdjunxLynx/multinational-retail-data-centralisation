from database_utils import DatabaseConnector as DBC
from data_extraction import DataExtractor as DE

class DataClean():
    def clean_user_data(self):
        DataFrame = DE().extract_rds_table(DBC().init_db_engine(), "orders_table")
        columns = DataFrame.columns.values.tolist()
        print(columns)
        print(DataFrame)
        for column in columns:
            for ind in DataFrame.index:
                print(DataFrame[column][ind])
                if DataFrame[column][ind] == "None":
                    DataFrame.drop(axis = 0, inplace = True, labels=ind)
                if DataFrame[column][ind] == "False":
                    DataFrame.drop(axis = 0, inplace = True, labels=ind)
        print(DataFrame)

DC = DataClean()
DC.clean_user_data()