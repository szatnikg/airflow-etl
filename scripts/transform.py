import os
import pandas as pd
import sys

sys.path.insert(0, os.path.abspath(r'/opt/airflow/scripts'))
from static_dir import StaticDirectory


class Transformator:
    def __init__(self, dirname):
        self.TEMP=dirname

    def enrich_w_weather(self, customerTable, weatherTable):
        # left join customer table with weatherDF
        result = pd.merge(customerTable, weatherTable, on='City', how='left')
        
        path=f'{self.TEMP}/Customers.parquet'

        result.to_parquet(path, index=False)
        if os.path.exists(path):
            print('\n=== CUSTOMERS TABLE ENRICHED WITH WEATHER DATA AND WRITTEN TO: ', path)
        else:
            raise ValueError('=== COULD NOT SAVE or NON EXISTENT TABLE:', path)

    def enrich_w_region(self, customerTable, regionTable):
        result = pd.merge(customerTable, regionTable, on='Country', how='left')
        
        path=f'{self.TEMP}/Customers.parquet'

        result.to_parquet(path, index=False)
        if os.path.exists(path):
            print('\n=== CUSTOMERS TABLE ENRICHED WITH REGION MAPPING DATA AND WRITTEN TO: ', path)
        else:
            raise ValueError('=== COULD NOT SAVE or NON EXISTENT TABLE:', path)
        
    def conversions(self, df):
        # Orders table only: convert to dates
        path=f'{self.TEMP}/Orders.parquet'

        df['RequiredDate'] = pd.to_datetime(df['RequiredDate'], errors='coerce').dt.date
        df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce').dt.date  
        df['ShippedDate'] = pd.to_datetime(df['ShippedDate'], errors='coerce').dt.date
        
        df.to_parquet(path, index=False)
        if os.path.exists(path):
            print('\n=== ORDERS TABLE DATES CONVERTED AND WRITTEN TO: ', path)
        else:
            raise ValueError('=== COULD NOT SAVE or NON EXISTENT TABLE:', path)

    def drop_duplicates(self, df, u_column, table_name):
        path=f'{self.TEMP}/{table_name}.parquet'

        u_df = df.drop_duplicates(subset=u_column, keep='first')
        
        u_df.to_parquet(path, index=False)
        if os.path.exists(path):
            print(f'\n=== {table_name} TABLE DROPPED DUPLICATES ON {u_column} AND WRITTEN TO: ', path)
        else:
            raise ValueError('=== COULD NOT SAVE or NON EXISTENT TABLE:', path)


def run():
    stat = StaticDirectory()
    
    exec = Transformator(stat.TEMP)
    # Drop duplicate if any on PK columns
    exec.drop_duplicates(stat.read_tmp_table('Customers'), 'CustomerID','Customers')
    exec.drop_duplicates(stat.read_tmp_table('Orders'), 'OrderID', 'Orders')
    # convert date
    exec.conversions(stat.read_tmp_table('Orders'))
    # enrich
    exec.enrich_w_weather(stat.read_tmp_table('Customers'), stat.read_tmp_table('weather'))
    exec.enrich_w_region(stat.read_tmp_table('Customers'), stat.read_tmp_table('region_mapping'))


if __name__=="__main__":
    run()