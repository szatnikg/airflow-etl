import pandas as pd
import os
from datetime import datetime



class StaticDirectory():
    def __init__(self):
        # call super for downstream initialization
        super().__init__()
        #  these are customer related but empty: I will consider out of scope> 'CustomerCustomerDemo' 'CustomerDemographics'
        self.EXTRACT_TABLES=('Orders','[Order Details]','Customers')
        self.ALL_TABLES=('Orders','[Order Details]','Customers', 'region_mapping')
        self.DATA_SOURCE = './data/src'
        self.TEMP = os.path.join(self.DATA_SOURCE, 'tmp')

        self.query_tables = "SELECT name FROM sqlite_master WHERE type='table';"
        self.create_log_file()

    def create_log_file(self):
        self.logpath = './data/sink/etl.log'
        if not os.path.exists(self.logpath):
            with open(self.logpath,'w') as logf:
                logf.write('==== LOG INIT ==== \n')
            return
        
    def read_tmp_table(self, name):
        path = os.path.join(self.TEMP, name+'.parquet')
        try:
            return pd.read_parquet(path)
        except Exception as err:
            print(err)
            raise ValueError('=== COULD NOT READ TABLE:', path)

    def get_top5_query(self, table):
        q = f'SELECT * FROM "{table}" LIMIT 5'
        return q
    

    def get_query(self, table):

        q = f"SELECT * FROM {table}"
        return q
    
    def append_log(self, text):
        log_message = f"{datetime.now().isoformat()} - {text} \n"

        with open(self.logpath, 'a',encoding='utf-8') as lf:
            lf.write(log_message)
            return