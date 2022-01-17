# ps-case-study
data engineering case study

Step 1: Access tick/minute data from Binance.US using their REST API and store it into a SQL database of your choosing. You can create a free account at Binance.US (https://www.binance.us/en/home) and set it up for API access. Download data from the earliest start date that is available. The ticker to be used is BTCUSDT. 

Step 2: The objective of this case study is to scan and clean the data. Identify potential discrepancies in the data that may exist and implement data validations checks.


btcusdt_downloader.py : download ohlcv data from binance and save to sql database (postgres)

btcusdt_cleaner.py : download data from sql and clean

Data Cleaning Notebook.ipynb : visuals and statistics on the data

config: binance.us api key/secret; postgres database password 
