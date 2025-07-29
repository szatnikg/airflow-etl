import great_expectations as gx
import pandas as pd 
import sys
import os

sys.path.insert(0, os.path.abspath(r'/opt/airflow/scripts'))
from static_dir import StaticDirectory


context = gx.get_context()
print(gx.__version__)
data_source = context.data_sources.add_pandas("pandas")
data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")
batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")


class Validator():
    def __init__(self, dirname):
        self.TEMP=dirname

    def get_expectations(self, table_name):
        expectations=[]
        #ORDERS TABLE
        if table_name=='Orders':
            #Expetations
            expectations=[
                gx.expectations.ExpectColumnValuesToBeOfType(column="OrderID", type_='int'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="CustomerID", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="EmployeeID", type_='int'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="OrderDate", type_='date'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="RequiredDate", type_='date'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="ShippedDate", type_='date'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="ShipVia", type_='int'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="Freight", type_='float'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="ShipName", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="ShipAddress", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="ShipPostalCode", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="ShipCity", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="ShipCountry", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="ShipRegion", type_='str'),
                
                # Checks for NULL values
                gx.expectations.ExpectColumnValuesToNotBeNull(column='OrderID'),
                gx.expectations.ExpectColumnValuesToNotBeNull(column='CustomerID'),
                gx.expectations.ExpectColumnValuesToNotBeNull(column='EmployeeID'),
                
                gx.expectations.ExpectColumnValuesToNotBeNull(column='OrderDate'),
                gx.expectations.ExpectColumnValuesToNotBeNull(column='RequiredDate'),
                gx.expectations.ExpectColumnValuesToNotBeNull(column='ShippedDate'),
                gx.expectations.ExpectColumnValuesToNotBeNull(column='ShipAddress'),
                gx.expectations.ExpectColumnValuesToNotBeNull(column='ShipCity'),
                gx.expectations.ExpectColumnValuesToNotBeNull(column='ShipCountry'),
                gx.expectations.ExpectColumnValuesToNotBeNull(column='ShipPostalCode'),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="ShipRegion"),
                

                # Check for uniquness
                gx.expectations.ExpectColumnValuesToBeUnique(column='OrderID')

            ]
            exist_columns=['OrderID', 'CustomerID', 'EmployeeID', 'OrderDate', 'RequiredDate',
                            'ShippedDate', 'ShipVia', 'Freight', 'ShipName', 'ShipAddress',
                            'ShipCity', 'ShipRegion', 'ShipPostalCode', 'ShipCountry']
            for col in exist_columns:
                expectations.append(gx.expectations.ExpectColumnToExist(column=col))
            return expectations
        
        #CUSTOMERS TABLE
        elif table_name=='Customers':


            expectations=[
                gx.expectations.ExpectColumnValuesToBeOfType(column="CustomerID", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="ContactName", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="CompanyName", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="ContactTitle", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="Address", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="Country", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="City", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="Region", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="PostalCode", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="Fax", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="Phone", type_='str'),


                # Checks for NULL values
                gx.expectations.ExpectColumnValuesToNotBeNull(column='City'),
                gx.expectations.ExpectColumnValuesToNotBeNull(column='CustomerID'),
                gx.expectations.ExpectColumnValuesToNotBeNull(column='Region'),

                # Check for uniquness
                gx.expectations.ExpectColumnValuesToBeUnique(column='CustomerID'),

                #Check Weather DATA #TODO

                gx.expectations.ExpectColumnValuesToBeOfType(column="temp", type_='float'),
                gx.expectations.ExpectColumnValuesToBeBetween( column='temp', min_value=-80, max_value=80),

                gx.expectations.ExpectColumnValuesToBeOfType(column="pressure", type_='float'),
                gx.expectations.ExpectColumnValuesToBeBetween( column='pressure', min_value=1000, max_value=1500),

                gx.expectations.ExpectColumnValuesToBeOfType(column="humidity", type_='float'),
                gx.expectations.ExpectColumnValuesToBeBetween( column='humidity', min_value=0, max_value=100),
               
                gx.expectations.ExpectColumnValuesToBeOfType(column="sea_level", type_='float'),
                gx.expectations.ExpectColumnValuesToBeBetween( column='sea_level', min_value=100, max_value=15000),

                gx.expectations.ExpectColumnValuesToBeOfType(column="temp_max", type_='float'),
                gx.expectations.ExpectColumnValuesToBeBetween( column='temp_max', min_value=-80, max_value=80),
                gx.expectations.ExpectColumnValuesToBeOfType(column="temp_min", type_='float'),
                gx.expectations.ExpectColumnValuesToBeBetween( column='temp_min', min_value=-80, max_value=80),

                gx.expectations.ExpectColumnValuesToBeOfType(column="grnd_level", type_='float'),
                gx.expectations.ExpectColumnValuesToBeBetween( column='grnd_level', min_value=10, max_value=15000),

                gx.expectations.ExpectColumnValuesToBeOfType(column="main", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="description", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="wind_speed", type_='float'),
                gx.expectations.ExpectColumnValuesToBeBetween( column='wind_speed', min_value=0, max_value=250)
                
            ]

            exist_columns=['CustomerID', 'CompanyName', 'ContactName', 'ContactTitle', 'Address',
                        'City', 'Region', 'PostalCode', 'Country', 'Phone', 'Fax'
                        ,'main', 'description', 'temp', 'pressure', 'humidity', 'sea_level',
                        'temp_max', 'temp_min', 'grnd_level', 'wind_speed']
            for col in exist_columns:
                expectations.append(gx.expectations.ExpectColumnToExist(column=col))
            return expectations

        #ORDER DETAILS TABLE
        elif table_name=='[Order Details]':
            expectations=[
                gx.expectations.ExpectColumnValuesToBeOfType(column="OrderID", type_='int'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="ProductID", type_='int'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="UnitPrice", type_='float'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="Quantity", type_='int'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="Discount", type_='float'),

                # Checks for NULL values
                gx.expectations.ExpectColumnValuesToNotBeNull(column='OrderID'),
                gx.expectations.ExpectColumnValuesToNotBeNull(column='ProductID'),
                gx.expectations.ExpectColumnValuesToNotBeNull(column='UnitPrice'),
                gx.expectations.ExpectColumnValuesToNotBeNull(column='Quantity'),
            ]

            exist_columns=['OrderID', 'ProductID', 'UnitPrice', 'Quantity', 'Discount']
            for col in exist_columns:
                expectations.append(gx.expectations.ExpectColumnToExist(column=col))
            return expectations


        elif table_name=='region_mapping':
            expectations=[
                gx.expectations.ExpectColumnValuesToBeOfType(column="Country", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="Region starting 2016", type_='str'),
                gx.expectations.ExpectColumnValuesToBeOfType(column="Region until 2017", type_='str'),

                # Checks for NULL values
                gx.expectations.ExpectColumnValuesToNotBeNull(column='Country'),
                gx.expectations.ExpectColumnValuesToNotBeNull(column='Region starting 2016'),
                gx.expectations.ExpectColumnValuesToNotBeNull(column='Region until 2017')
            ]

            exist_columns=['Country', 'Region until 2017', 'Region starting 2016']
            for col in exist_columns:
                expectations.append(gx.expectations.ExpectColumnToExist(column=col))
            return expectations

        else:
            raise ValueError('DEFINE YOUR TABLE in queries.py self.EXTRACT_TABLES=(Orders,[Order Details],Customers) + region_mapping')
        

    def validate(self, table_name, df):
        batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
        dummy=[]

        suite_name = f"{table_name}_suite"
        suite = gx.ExpectationSuite(name=suite_name)
        suite = context.suites.add(suite)
            
        for i in self.get_expectations(table_name):
            suite.add_expectation(i)

        validation_result = batch.validate(suite)
        
        appendix='##ERROR##'

        print('==== validating table:',table_name)
        for test in validation_result['results']:
            if test['success']==False:
                print(appendix,test['success'], ' #### Failed on ',test['expectation_config']['type'] ,' col: ', test['expectation_config']['kwargs']['column'])
            else:
                pass
                #print(' ==== ',test['success'], ' ==== ',test['expectation_config']['type'],' col: ', test['expectation_config']['kwargs']['column'])
        print(validation_result['statistics'])

def run():
    stat = StaticDirectory()
    
    exec = Validator(stat.TEMP)

    for table_name in stat.EXTRACT_TABLES:
        exec.validate(table_name, stat.read_tmp_table(table_name))
    
    #region mapping
    exec.validate('region_mapping', stat.read_tmp_table('region_mapping'))



if __name__=="__main__":
    run()