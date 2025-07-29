from datetime import timedelta, datetime
import os
import sqlite3
import pandas as pd


import sys
sys.path.insert(0, os.path.abspath(r'/opt/airflow/scripts'))

# own imports
from static_dir import StaticDirectory

class DbHandler(StaticDirectory):
    """Handling SQLlite interactions, data read, extract src"""

    def __init__(self, db_src):
        # call super for downstream initialization
        super().__init__()
        # Connect to the DB
        self.conn = sqlite3.connect(db_src)
        self.c = self.conn.cursor()

        
    def close_conn(self):
        # Close the connection
        self.c.close()

class ExtractTables(DbHandler):
    """Extract from source xlsx, sqllite.db"""
    def __init__(self, db_src):
        # call super for downstream initialization
        super().__init__(db_src)

        
        if not os.path.exists(self.TEMP):
            os.mkdir(self.TEMP)

    def get_tables(self):
            # Q: which tables to load?
            # Query to get all table names
            self.c.execute(self.query_tables)

            tables = self.c.fetchall()

            # Print table names
            for table in tables:
                print(table[0]) 
            return tables

    def read_region_mapping(self, excel_name):
        path=f'{self.TEMP}/{excel_name}.parquet'
        region_map_df = pd.read_excel(f'{self.DATA_SOURCE}/{excel_name}.xlsx')
        
        region_map_df.to_parquet(path, index=False)
        if os.path.exists(path):
            print('=== EXTRACTED TABLE WRITTEN TO: ', path)

    def extract_tables(self):
        # use yield generator / df writes to disk ?
        # so no memory overload happens (in case of big tables) - here it is not applicable
        for table in self.EXTRACT_TABLES:
            path=f'{self.TEMP}/{table}.parquet'
            df = pd.read_sql_query(self.get_query(table), self.conn)
            df.to_parquet(path, index=False)
            if os.path.exists(path):
                print('=== EXTRACTED TABLE WRITTEN TO: ', path)



def run():
    #provide DBSRC as docker env variable
    DB_SRC = "./data/src/northwind/dist/northwind.db"
    EXCEL_NAME_NOEXT = 'region_mapping'
    executor = ExtractTables(DB_SRC)
    executor.read_region_mapping(EXCEL_NAME_NOEXT)
    executor.extract_tables()


#if __name__=='__main__':
    #run()