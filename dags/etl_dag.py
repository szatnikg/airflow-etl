
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys
from airflow.utils.trigger_rule import TriggerRule

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

def read_log():
    from scripts import read_log
    read_log.run()

default_args = {
    'owner': 'Gergo',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    }

# Define the DAG
with DAG(
    dag_id="etl_pipeline_dag",
    default_args=default_args,
    schedule='@daily',  
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

    log_task = PythonOperator(
        task_id='read_log',
        python_callable=read_log,
        trigger_rule=TriggerRule.ALL_DONE,  # This ensures it runs no matter what
    )

    # Define task dependencies
    extract_task >> weather_task >> transform_task >> validate_task >> load_task >> log_task

