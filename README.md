# Building a Containerized ETL Pipeline
This project demonstrates how to build a containerized ETL (Extract, Transform, Load) pipeline using modern tools and technologies. The pipeline extracts data from multiple sources, transforms it by merging, enriching, and validating, and finally loads it into a target SQLite database. It also includes logging and monitoring capabilities.

## **Get Started**
Follow the steps below to set up and run the ETL pipeline:
1. **Create and Navigate to a Working Directory**:
``` bash
    mkdir poc
    cd poc
```
2. **Clone the Repository**:
```bash
   git clone https://github.com/szatnikg/airflow-etl
   cd airflow-etl
```
3. **Update the Weather API Key**:
   - Open the `.env` file in the project directory.
   - Replace the default `API_key` with your own API key for the Weather API. [OpenWeatherMap](https://openweathermap.org/).

4. **Build and Start the Docker Containers**:
 ```bash
    docker compose up --build -d
```
5. **Access the Airflow Web Interface**:
    - Open your browser and go to `http://localhost:8080`.
    - Log in with the following credentials: Username: `airflow` , Password `airflow`  
    - Navigate to the **DAGs** tab to monitor the ETL pipeline. Wait for the DAG to trigger and observe its progress.

6. **Monitor the Automatic ETL DAG Trigger**:
    - The ETL DAG (Directed Acyclic Graph) is automatically triggered with a delay of 30 seconds.
    - To check its status, run:
```bash
docker compose logs airflow-trigger-dag
```
## **Use Case**
The ETL pipeline performs the following tasks:
1. **Extract**:

    - Fetch data from multiple sources:
        - A local SQLite3 database (`SQLlite3.db`). /scripts/extract.py
        - An Excel file (`.xlsx`). /scripts/extract.py
        - Weather data from the [OpenWeatherMap API](https://openweathermap.org/). /scripts/weather.py
2. **Transform**:

    - Merge and enrich the extracted data. /scripts/transform.py 
    - `pandas` and `Great Expectations`. 
    - Validate schema and input columns /scripts/validate.py   
    - Log all operations for traceability. /scripts/read_log.py
3. **Load**:

    - Load the transformed data into a target SQLite database. /scripts/load.py

By following the steps above, you can set up and run a fully functional ETL pipeline that integrates multiple data sources, performs transformations, and loads the results into a database.

## **Technologies Used - Justification**
**Airflow**: Orchestrates the ETL pipeline.
    - it is a mature technology (broad community) to provide scalable run time for data pipelines, also integrates with cloud solutions. 

**Great Expectations**: Validates and ensures data quality.
    - It has a well defined framework for managing data from source to target and helps data engineers remove their custom logic and create standardized data validation scripts.

**pandas**: Handles data manipulation and transformation.
    - Mature module to handle all local data movements. Pandas based knowledge is interpretable (to some extent) to pyspark -dataframe knowledge which is crucial today.  

**aiohttp**: Fetches data asynchronously from the Weather API.
    - It creates coroutines to fetch data faster while waiting for the responses (API-rate limits shall be considered)  

**Docker**: Containerizes the entire application for easy deployment.
    - Industry standard containerization tool.

**SQLite3**: Serves as both the source and target database.
    - Using as target database, due to ease of management: local instance can be created via just a simple command and dataframes can be loaded and queried via SQL-scripts. Perfect for a fast PoC / never use for PROD. 

## **Challenges**

**Building custom container inside composed airflow services:**
- smart management of the sys path for import own python modules:
    - for dags folder: sys.path.insert(0, os.path.abspath(r'/opt/airflow/'))
    - for scripts folder: opt/airflow/scripts 
- triggering a scheduled dag via new container executing a shell script
    - since the dag is already has a daily schedule, it is enough to unpause and not trigger it otherwise it starts twice.

**Notes**
- I created a coordinates.json file that can solve for future processing needs as the geomapping of cities from weatherAPI are not changing their coordinate values, so after first run, we use the Geomapping only for new cities. (Reduce # of requests)
- I am failing the pipeline if 50%-or less cities can be matched from weather api data.
- Each script file (mapped into airflow task) will create a traceback error log mentioning the name of the script file too. (try-except of each /script/*.py )
- Absence of time to denote: exploratory data analysis has not been performed on the enriched dataset.

**Data validation - expectations suite**
- Also see first point of Future Improvements below
- Validation runs on 4 tables: Orders, [Order Details] ,Customers, region_mapping.xlsx
- Checkink of uniqueness in
    Orders table: OrderID
    Customer table: CustomerID
- Checking weather data types and ranges:
    E.g.: For temp: (column='temp', min_value=-80, max_value=80)

- Checking relevant tables not be null (always on PK,FK columns)
- Checking all table types
- Checking all tables to exist
  

## **Future Improvements**
**Create an exemption function for great expectation**
- Currently great_expectation validations are only for logs. In orther to fail the pipeline based on unmatched expectations some exemption logic based on business requirements shall be made. 
- (let the pipeline go but send email to Data Steward etc. ) this would be handled via new Airflow tasks 

**Git history contains secrets**
- This git repo will contain API secret key in the commit history - prune it.

**Refactor/ improve code**
- code refactoring is needed for integration with staticDirectory: transform.py, validator.py
- I would create expectations after the extract to see Primary Key duplicates or Nulls and fail the pipelines. (this is currently handled via dropping them)

**TMP directory**
- read / writes to disk: maybe a dynamic rule could be made when to write to disk (only above certain size)
- (considerations: IO to storage accounts costs in cloud)
- also I was overwriting the files (in case of a Data lake instead of the overwrite it is written to a new layer bronze-silver-gold)
- TMP directory is not cleand after the load - this should be included.

**Airflow Task** 
- in case airflow - task decorator could have been used to pass variables.

**Container**
- Specify hardware resources for containers: (Based on business need) --cpus="Y" --memory="X" 

## **Current data quality issues**
- expect_column_values_to_not_be_null Fails on following: ShipPostalCode, ShippedDate, RequiredDate, OrderDate
- Weather API sometimes can't receive data for Tsawassen city. In my logs: === ##WARNING## No data available for Tsawassen
