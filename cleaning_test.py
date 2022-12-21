import pandas as pd
pd.set_option('display.max_columns', None)
flights_df = pd.read_csv("flights.txt", sep="|") # Make sure flights.txt is in the same directory