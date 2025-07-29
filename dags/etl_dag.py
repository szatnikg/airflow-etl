
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys
sys.path.insert(0, os.path.abspath(r'/opt/airflow/'))

import logging

# Suppress Great Expectations logs
logging.getLogger("great_expectations").setLevel(logging.ERROR)


# Task functions that import and run your scripts
def run_extract():
    print(os.getcwd())
    from scripts import extract
    extract.run()

def run_validate():
    from scripts import validate
    validate.run()

def run_load():
    from scripts import load
    load.run()

def run_weather():
    from scripts import weather
    weather.run()

def run_transform():
    from scripts import transform
    transform.run()

# Define the DAG
with DAG(
    dag_id="etl_pipeline_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Manual trigger
    catchup=False,
    tags=["etl", "poc"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=run_extract,
    )

    weather_task = PythonOperator(
        task_id="weather",
        python_callable=run_weather,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
    )

    validate_task = PythonOperator(
        task_id="validate",
        python_callable=run_validate,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=run_load,
    )

    # Define task dependencies
    extract_task >> weather_task >> transform_task >> validate_task >> load_task

