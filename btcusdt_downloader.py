# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from binance.client import Client
from binance.exceptions import BinanceAPIException
import datetime
import pandas as pd
import psycopg2
import sqlalchemy
import numpy as np
import config



def get_klines(client,start,end):    
    '''
    Use binance-python (wrapper for binance API) to pull historical klines between
    start and end.

    Parameters
    ----------
    client : binance.client
    start : str | int
        str: 1 Jan, 2001.
        int: millisecond timestamp.
    end : str | int
        str: 1 Jan, 2001.
        int: millisecond timestamp..
        
    Returns
    -------
    klines : json format list of list
        Error code: -1.
    '''
    print("Requesting klines from {start} : {end}".format(start=start,end=end))
    try: 
        klines = client.get_historical_klines(symbol = "BTCUSDT", interval=Client.KLINE_INTERVAL_1MINUTE, start_str = start, end_str=end)
        print("Request successful")
        return klines
    except BinanceAPIException as e:
        print(e.message)
        return -1, start, end
    

def klines_to_pd(data, headers):
    '''
    turn klines from json to pandas dataframe

    Parameters
    ----------
    data : list of list (json)
    headers : list of string
        column names for the returned dataframe.

    Returns
    -------
    df : pandas.dataframe
        all entries cast to float.
    '''
    df = pd.DataFrame.from_records(data,columns = headers)
    df = df.astype(float)
    return df

def pd_to_sql(df, table_name, connection_params):
    '''
    

    Parameters
    ----------
    df : pandas.dataframe
    table_name : name of table to append to/create in postgres sql
    connection_params : other connection parameters to establish postgres connection
        required: dbname, username, password

    Returns
    -------
    None 
        Error code: -1
    '''
    engine = sqlalchemy.create_engine(
        'postgresql://{user}:{password}@localhost:5432/{dbname}'.format(**connection_params)
     )
    print("Writing to SQL DB ....")
    try:
        df.to_sql(table_name, engine, if_exists="append", index=False)
        print("Commit Successful")
        return
    except psycopg2.DatabaseError as e:
        print(e)
        return -1
        

class btcusdt_downloader:
        
    def __init__(self,api_key,api_secret,pwd,data_start_date,connection_params,table_name,headers):
        self.api_key = api_key
        self.api_secret = api_secret
        self.pwd = pwd
        self.data_start_date = data_start_date
        self.connection_params = connection_params
        self.table_name = table_name
        self.headers = headers
        self.client = None
        self.failed_chunk = []
        
    def connect_binance(self):
        self.client = Client(self.api_key,self.api_secret)
        return         
        
    def initialize_db(self,end_date=None,chunksize="M"):    
        '''
        Parameters
        ----------
        end_date : str | int
        chunksize : str
            use "chunking" to manage size of query. Default to monthly chunks.
            failed chunks stores failed queries (start,end)
            
        Returns
        -------
        None
            Error code : -1
        '''
        if self.client is None:
            print("Binance client not connected")
            return -1
        curr_end_time = 0
        end_date = datetime.datetime.today() if end_date is None else end_date
        date_range = pd.date_range(start=self.data_start_date,end=end_date,freq=chunksize)
        for i in range(len(date_range)-1):
            start, end = date_range[i].strftime("%d %b, %Y"), date_range[i+1].strftime("%d %b, %Y")
            print((i, start,end))
            res = self._get_klines(start, end, curr_end_time)
            if res == -1:
                self.failed_chunk.append((start,end))
            else:
                curr_end_time = res
        return 
    
    def update_db(self):
        '''
        Updates database to current

        Returns
        -------
        None
            Error code : -1.

        '''
        print("Updating db to today")
        connection = psycopg2.connect(**self.connection_params)
        cursor = connection.cursor()
        cursor.execute("SELECT MAX(open_time) FROM {}".format(self.table_name))
        res = [row for row in cursor]
        connection.close()
        if len(res)!=1 and len(res[0])==1:
            print("Timestamp error")
            return -1
        latest_timestamp = res[0][0]
        res = self._get_klines(int(latest_timestamp), int(datetime.datetime.today().timestamp() * 1000), int(latest_timestamp))
        if res == -1:
            self.failed_chunk.append((int(latest_timestamp), int(datetime.datetime.today().timestamp() * 1000)))
            return -1
        return 
    
    def _get_klines(self,start,end,prev):
        '''
        Downloads klines data from binance. Helper method to manage repeated code. 

        Parameters
        ----------
        start : str | int
        end : str | int
        prev : int
            most recent timestamp. Only appends data later than prev. 
            
        Returns
        -------
        int 
            latest timestamp after adding new data to sql
            Error code : -1
        '''
        klines = get_klines(self.client, start, end)
        if klines[0] == -1:
            print("failed at : " + str(start) + str(end))
            return -1
        df = klines_to_pd(klines,self.headers)
        df["open_time"].astype(np.int64)
        df["close_time"].astype(np.int64)
        df = df.loc[df["open_time"] > prev]
        df = df.drop(labels="ignore",axis=1)
        res = pd_to_sql(df, self.table_name, self.connection_params)
        return -1 if res == -1 else max(df["open_time"])
        


def run():
    api_key = config.api_key
    api_secret = config.api_secret
    pwd = config.pwd
    data_start_date = "1 July, 2017"
    
    headers = [s.replace(" ","_") for s in ["open time", "open", "high", "low",	"close", "volume", "close time", "quote asset volume",
               "number of trades", "taker buy base asset volume", "taker buy quote asset volume", "ignore"]]
    connection_params = {
        "dbname" : "postgres",
        "user" : "postgres",
        "password" : pwd
        }
    table_name = "klines"
    
    
    loader = btcusdt_downloader(api_key,api_secret,pwd,data_start_date,connection_params,table_name,headers)
    loader.connect_binance()
    # loader.initialize_db()  # run once
    loader.failed_chunk
    loader.update_db()  
    
# run()


# [
#   [
#     1499040000000,      // Open time
#     "0.00386200",       // Open
#     "0.00386200",       // High
#     "0.00386200",       // Low
#     "0.00386200",       // Close
#     "0.47000000",  // Volume
#     1499644799999,      // Close time
#     "0.00181514",    // Quote asset volume
#     1,                // Number of trades
#     "0.47000000",    // Taker buy base asset volume
#     "0.00181514",      // Taker buy quote asset volume
#     "0" // Ignore.
#   ]
# ]



