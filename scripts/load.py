import traceback
from datetime import timedelta, datetime
import os
import sqlite3
import pandas as pd


import sys
sys.path.insert(0, os.path.abspath(r'/opt/airflow/scripts'))

from static_dir import StaticDirectory
from extract import DbHandler

class Loader:
    def __init__(self, db):
        self.statDirAndDB = db
        self.conn = db.conn
        self.c = self.conn.cursor()

    def load(self, table_name, df):

        df.to_sql(table_name, self.conn, if_exists="replace", index=False)

    def check_results(self, query, table_name):
        df = pd.read_sql_query(query, self.conn)
        if len(df) == 5:
            self.statDirAndDB.append_log(f'=== {table_name} TABLE IN TARGET DB OK ===')
            print(f'=== {table_name} TABLE IN TARGET DB OK ===')
        else:
            raise ValueError(f'=== TABLE NAME {table_name} =  ERROR IN TARGET DB: no records ===')

def run():
    try:
        db = DbHandler(os.getenv('DB_TARGET'))

        loader = Loader(db)
        for table_name in db.ALL_TABLES:
            loader.load(table_name, db.read_tmp_table(table_name))
        # validate results
        db.get_tables()
        for table_name in db.ALL_TABLES:
            loader.check_results(db.get_top5_query(table_name), table_name) 

    except Exception as MyErr:
        
        stat = StaticDirectory()
        stat.append_log('ERROR in load.py: '+str(MyErr))
        
        stat.append_log(traceback.format_exc())
        raise ValueError(MyErr)

if __name__=='__main__':
    run()