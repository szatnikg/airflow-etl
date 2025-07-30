import traceback
from datetime import timedelta, datetime
import os
import sqlite3
import pandas as pd

import sys
sys.path.insert(0, os.path.abspath(r'/opt/airflow/scripts'))
# own imports
from static_dir import StaticDirectory



class DbHandler(StaticDirectory):
    """SQLlite init"""

    def __init__(self, db_src):
        # call super for downstream initialization
        super().__init__()
        # Connect to the DB
        self.conn = sqlite3.connect(db_src)
        self.c = self.conn.cursor()
    
    def get_tables(self):
        # Q: which tables to load?
        # Query to get all table names
        self.c.execute(self.query_tables)

        tables = self.c.fetchall()

        # Print table names
        for table in tables:
            print(table[0]) 
        return tables

        
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



    def read_region_mapping(self, excel_name):
        path=f'{self.TEMP}/{excel_name}.parquet'
        region_map_df = pd.read_excel(f'{self.DATA_SOURCE}/{excel_name}.xlsx')
        
        region_map_df.to_parquet(path, index=False)
        if os.path.exists(path):
            self.append_log('=== EXTRACTED TABLE WRITTEN TO: '+ path)
            print('=== EXTRACTED TABLE WRITTEN TO: ', path)
        else:
            raise ValueError('=== EXTRACTED TABLE COULD NOT BE WRITTEN TO: ' +path)

    def extract_tables(self):
        # use yield generator / df writes to disk ?
        # so no memory overload happens (in case of big tables) - here it is not applicable
        for table in self.EXTRACT_TABLES:
            path=f'{self.TEMP}/{table}.parquet'
            df = pd.read_sql_query(self.get_query(table), self.conn)
            df.to_parquet(path, index=False)
            if os.path.exists(path):
                self.append_log('=== EXTRACTED TABLE WRITTEN TO: '+ path)
                print('=== EXTRACTED TABLE WRITTEN TO: ', path)
            else:
                #this gets wriiten to log via run() except case
                raise ValueError('=== EXTRACTED TABLE COULD NOT BE WRITTEN TO: ' +path)

def run():
    EXCEL_NAME_NOEXT = 'region_mapping'   
    try:
        executor = ExtractTables(os.getenv('DB_SRC')) 
        executor.read_region_mapping(EXCEL_NAME_NOEXT)
        executor.extract_tables()
        executor.close_conn()
    
    except Exception as MyErr:
        sd = StaticDirectory()
        sd.append_log('ERROR in extract.py: '+str(MyErr))
        stat.append_log(traceback.format_exc())
        raise ValueError(MyErr)

if __name__=='__main__':
    run()