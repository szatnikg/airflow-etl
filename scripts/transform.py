import traceback
import os
import pandas as pd
import sys

sys.path.insert(0, os.path.abspath(r'/opt/airflow/scripts'))
from static_dir import StaticDirectory


class Transformator:
    def __init__(self, dirname, statDir):
        self.TEMP=dirname
        self.statDir = statDir

    def enrich_w_weather(self, customerTable, weatherTable):
        # left join customer table with weatherDF
        result = pd.merge(customerTable, weatherTable, on='City', how='left')
        
        path=f'{self.TEMP}/Customers.parquet'

        result.to_parquet(path, index=False)
        if os.path.exists(path):
            print('\n=== CUSTOMERS TABLE ENRICHED WITH WEATHER DATA AND WRITTEN TO: ', path)
            self.statDir.append_log('=== CUSTOMERS TABLE ENRICHED WITH WEATHER DATA AND WRITTEN TO: '+ path)
        else:
            raise ValueError('=== COULD NOT SAVE or NON EXISTENT TABLE:', path)

    def enrich_w_region(self, customerTable, regionTable):
        result = pd.merge(customerTable, regionTable, on='Country', how='left')
        
        path=f'{self.TEMP}/Customers.parquet'

        result.to_parquet(path, index=False)
        if os.path.exists(path):
            self.statDir.append_log('=== CUSTOMERS TABLE ENRICHED WITH REGION MAPPING DATA AND WRITTEN TO: '+ path)
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
            self.statDir.append_log('=== ORDERS TABLE DATES CONVERTED AND WRITTEN TO: '+ path)
            print('\n=== ORDERS TABLE DATES CONVERTED AND WRITTEN TO: ', path)
        else:
            raise ValueError('=== COULD NOT SAVE or NON EXISTENT TABLE:', path)

    def drop_customer_city_region_nulls(self, df):
        # Customers table business use case defined: city and region should not be empty
        path=f'{self.TEMP}/Customers.parquet'
        
        duplicates = df[df[['City', 'Region']].isnull().any(axis=1)]
        self.statDir.append_log("=== Number of null rows in CUSTOMERS TABLE CITY, REGION columns: {len(duplicates)}")
        print(f"\n=== Number of null rows in CUSTOMERS TABLE CITY, REGION columns: {len(duplicates)}")

        # Drop rows with nulls in 'City' or 'Region'
        df = df.dropna(subset=['City', 'Region'])

        duplicates_check = df[df[['City', 'Region']].isnull().any(axis=1)]
        if len(duplicates_check):
            raise ValueError(f"\n=== ERROR COULD NOT DROP NULLS Number of null rows in CUSTOMERS TABLE CITY, REGION columns: {len(duplicates)}")
        
        df.to_parquet(path, index=False)
        if os.path.exists(path):
            self.statDir.append_log('=== DROPPED NULLS CUSTOMERS TABLE CITY, REGION columns AND WRITTEN TO: '+ path)
            print('\n=== DROPPED NULLS CUSTOMERS TABLE CITY, REGION columns AND WRITTEN TO: ', path)
        else:
            raise ValueError('=== COULD NOT SAVE or NON EXISTENT TABLE:', path)

    def drop_duplicates(self, df, u_column, table_name):
        path=f'{self.TEMP}/{table_name}.parquet'

        u_df = df.drop_duplicates(subset=u_column, keep='first')
        
        u_df.to_parquet(path, index=False)
        if os.path.exists(path):
            self.statDir.append_log(f'=== {table_name} TABLE DROPPED DUPLICATES ON {u_column} AND WRITTEN TO: '+ path)
            print(f'\n=== {table_name} TABLE DROPPED DUPLICATES ON {u_column} AND WRITTEN TO: ', path)
        else:
            raise ValueError('=== COULD NOT SAVE or NON EXISTENT TABLE:', path)

def run():

    stat = StaticDirectory()
    try:

        exec = Transformator(stat.TEMP, stat)
        # Drop duplicate if any on PK columns
        exec.drop_duplicates(stat.read_tmp_table('Customers'), 'CustomerID','Customers')
        exec.drop_duplicates(stat.read_tmp_table('Orders'), 'OrderID', 'Orders')

        exec.drop_customer_city_region_nulls(stat.read_tmp_table('Customers'))

        # convert date
        exec.conversions(stat.read_tmp_table('Orders'))
        # enrich
        exec.enrich_w_weather(stat.read_tmp_table('Customers'), stat.read_tmp_table('weather'))
        exec.enrich_w_region(stat.read_tmp_table('Customers'), stat.read_tmp_table('region_mapping'))
    except Exception as MyErr:
        stat.append_log('ERROR in transform.py: '+str(MyErr))
        stat.append_log(traceback.format_exc())
        raise ValueError(MyErr)


if __name__=="__main__":
    run()