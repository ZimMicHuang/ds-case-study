# -*- coding: utf-8 -*-
"""
Created on Sun Jan 16 22:42:13 2022

@author: micke
"""

import pandas as pd
import sqlalchemy
import config


class btcusdt_cleaner:
    
    def __init__(self,table_name,connection_params):
        self.table_name = table_name
        self.connection_params = connection_params
        self.df = None
        self.ts = None
        return 
    
    def get_data(self):
        engine = sqlalchemy.create_engine(
            'postgresql://{user}:{password}@localhost:5432/{dbname}'.format(**self.connection_params)
         )
        self.df = pd.read_sql_table(
            self.table_name,
            con=engine)
        return
    
    def basic_clean(self):
        '''
        checks performed:
            (warning)
            - repeated ohlcv: are ohlcv repeated from previous values?
            - 0 volume: do intervals represent 0 volume?
            - stale_ohlc: are ohlc 0 variance?
            
            (drop)
            - kline width: do intervals have negative width?
            - time step check: does open_time overlap with previous close?
        
        Output:
            appends "status": definition: 0 (fine), 1 (warning), 2 (drop)
            
        Returns
        -------
        None.
        '''    
        repeated_value = (self.df[['open', 'high', 'low', 'close', 'volume']] == 
                                self.df[['open', 'high', 'low', 'close', 'volume']].shift(1)).sum(axis=1)  # repeated OHLCV from previous?
        stale_ohlc = self.df[['open', 'high', 'low', 'close']].std(axis=1) <= 0.000000001 # ohlc 0 variance        
        
        # status definition: 0 (fine), 1 (warning), 2 (drop)
        drop = (self.df["close_time"] - self.df["open_time"]) < 0.000000001
        drop |= (self.df["open_time"] - self.df["open_time"].shift(1)) < 0.000000001 
        
        warning = repeated_value>=5  # repeated ohlcv
        warning |= self.df["volume"] < 0.000000001 # 0 volume
        warning |= stale_ohlc
        
        self.df["status"] = (warning * 1) + (drop * 1)
        
        print("total records with warning: " + str(sum(self.df["status"]==1)))
        print("total records with recommended drop: " + str(sum(self.df["status"]==2)))
        return
    
    def get_ts(self):
        '''
        Extract minute-level time series data from dataframe. "Merge" open and close price.
        
        Output saved in self.ts
        
        Returns
        -------
        None.

        '''
        df = self.df
        close_px = df["close"]
        close_px.index = pd.to_datetime(df["close_time"],unit="ms").round("1min")
        close_px = close_px[~close_px.index.duplicated(keep='first')]

        open_px  = df[["open","status"]]
        open_px.index = pd.to_datetime(df["open_time"],unit="ms").round("1min")
        open_px = open_px[~open_px.index.duplicated(keep='first')]

        open_px = open_px.reindex(pd.date_range(open_px.index[0],close_px.index[-1],freq="1min"))
        close_px = close_px.reindex(pd.date_range(open_px.index[0],close_px.index[-1],freq="1min"))

        open_px.loc[open_px["open"].isna(),"open"] = close_px
        self.ts = open_px.rename(columns={"open":"px"})
        self.ts.loc[self.ts.isna().sum(axis=1)>0,"status"] = 4
        return 
        
    def ts_clean(self,k1,w1,k2,w2):
        '''
        checks performed:
            (backfill)
            - reindex to 60/24/7/365 and fill na with linear interpolation. interpolated values are flagged as "status: 4 (backfill)"
            
            (discontinuity)
            - accept data point if "close" within k1 std (of px) of w1_moving; flag as discontinuous otherwise
            - accept data point if "close" within k2 std (of px) of w2_moving; flag as discontinuous otherwise     
            
        output:
            - appends MA columns to df: ma, lb, ub, %b for each rolling window
            - updates "status" column: definition: 0 (fine), 3 (discontinuous), 4(backfill) 
            
         Returns
         -------
         None.
        '''           
        self.ts["ma{w1}".format(w1=w1)] = self.ts["px"].rolling(window=w1).mean()
        self.ts["ub{w1}".format(w1=w1)] = self.ts["ma{w1}".format(w1=w1)] + self.ts["px"].rolling(window=w1).std() * k1
        self.ts["lb{w1}".format(w1=w1)] = self.ts["ma{w1}".format(w1=w1)] - self.ts["px"].rolling(window=w1).std() * k1
        self.ts["%ma{w1}".format(w1=w1)] = self.ts["px"] / self.ts["ma{w1}".format(w1=w1)]
        
        self.ts["ma{w2}".format(w2=w2)] = self.ts["px"].rolling(window=w2).mean()
        self.ts["ub{w2}".format(w2=w2)] = self.ts["ma{w2}".format(w2=w2)] + self.ts["px"].rolling(window=w2).std() * k2
        self.ts["lb{w2}".format(w2=w2)] = self.ts["ma{w2}".format(w2=w2)] - self.ts["px"].rolling(window=w2).std() * k2
        self.ts["%ma{w2}".format(w2=w2)] = self.ts["px"] / self.ts["ma{w2}".format(w2=w2)]
        
        discontinuous = ( (self.ts["px"] > self.ts["ub{w1}".format(w1=w1)]) \
            | (self.ts["px"] > self.ts["ub{w2}".format(w2=w2)]) \
            | (self.ts["px"] < self.ts["lb{w1}".format(w1=w1)]) \
            | (self.ts["px"] < self.ts["lb{w2}".format(w2=w2)])) \
            & ((self.ts["status"] == 0) | (self.ts["status"] == 2))
            
        # self.ts[0:50000].plot()
        self.ts.loc[discontinuous,"status"] = 3
        print("total records with potential discontinuity: " + str(sum(self.ts["status"]==3)))
        print("total records with missing data/interpolation: " + str(sum(self.ts["status"]==4)))
        return
    
    def get_report(self):
        print("Dimension of OHLCV dataframe: ({rows},{cols})".format(rows=self.df.shape[0],cols=self.df.shape[0]))
        print("Dimension of time series: ({rows},{cols})".format(rows=self.ts.shape[0],cols=self.ts.shape[0]))
        print("total records with warning: " + str(sum(self.df["status"]==1)))
        print("total records with recommended drop: " + str(sum(self.df["status"]==2)))
        print("total records with potential discontinuity: " + str(sum(self.ts["status"]==3)))
        print("total records with missing data/interpolation: " + str(sum(self.ts["status"]==4)))
        return
    
    
def run():   
    pwd = config.pwd
    connection_params = {
        "dbname" : "postgres",
        "user" : "postgres",
        "password" : pwd
        }
    table_name = "klines"
    
    cleaner = btcusdt_cleaner(table_name,connection_params)
    cleaner.get_data()
    cleaner.basic_clean()
    cleaner.get_ts()
    cleaner.ts_clean(10, 60, 10, 300)
    cleaner.get_report()
    
run()