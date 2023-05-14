# multinational-retail-data-centralisation

##
This script uses `pandas`, `numpy` and a PostGreSQL server to download data from various sources, combine them all into one DataFrame, and then clean them

### data_extraction.py
#### This script is used to demonstrate various different ways of accessing data, and then cleaning them.
1. It creates a class that lists all the tables in a `PSQL server` and then downloads all data from them
2. It then uses the module `requests` to query an online API and recieves data.
3. It then accesses an online AWS S3 bucket using `boto3`, downloading the data into a file called `product_list.csv`
4. Finally it extracts all the data from a `.pdf` file

### data_cleaning.py
#### This script combines all the data stored in `data_extraction.py` and combines them all into one `Pandas Dataframe`
Pandas functions are then used to clean the data in numerous ways such as:
1. ensuring the phone numbers are of format `xxxxxxxxxxx` where x are numerals. This gets rid of any country codes, bad formatting etc
2. ensuring all dates are in the format `YYYY-MM-DD` and does not contain and strings such as `March`
3. ensuring the emails have a username and domain. The domain is not being checked, but could be changed to only accept values such as `gmail.com` or `hotmail.com` etc
and other cleaning functions for the other data sources. Once cleaned, the script uploads to a new PSQL Server

###database_utils.py
#### small module/scripts that allows easy functions to connect, upload and read data from a `PostGreSQL` server.
The PSQL and AWS S3 bucket credentials are stored in a `.yaml` file to access the data
