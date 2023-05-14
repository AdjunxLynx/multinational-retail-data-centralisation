# multinational-retail-data-centralisation

##
This script uses `pandas`, `numpy` and a PostGreSQL server to download data from various sources, combine them all into one DataFrame, and then clean them

### data_extraction.py
#### This script is used to demonstrate various different ways of accessing data, and then cleaning them.
1. It creates a class that lists all the tables in a PSQL server and then downloads all data from them
2. It then uses the module `requests` to query an online API and recieves data.
3. It then accesses an online AWS S3 bucket using `boto3`, downloading the data into a file called `product_list.csv`
4. Finally it extracts all the data from a `.pdf` file

### data_cleaning.py
####
