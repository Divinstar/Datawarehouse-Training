from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import shutil
import logging
import os

INPUT_DIR = '/opt/airflow/data/input'
OUTPUT_DIR = '/opt/airflow/data/output'
TIMEOUT = 600  

default_args = {
    'owner': 'airflow',
    'retries': 1,
}


def csv_file_exists():
    for filename in os.listdir(INPUT_DIR):
        if filename.endswith('.csv'):
            return True
    return False


def process_file():
    logging.info("Processing CSV files...")
    


def archive_csvs():
    for filename in os.listdir(INPUT_DIR):
        if filename.endswith('.csv'):
            src = os.path.join(INPUT_DIR, filename)
            dst = os.path.join(OUTPUT_DIR, filename)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.move(src, dst)
            logging.info(f"Moved {filename} to output.")

with DAG(
    dag_id='any_csv_file_pipeline',
    default_args=default_args,
    description='Wait for any CSV in input folder, process and move to output',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['assignment', 'sensor'],
) as dag:

    wait_for_any_csv = PythonSensor(
        task_id='wait_for_any_csv_file',
        python_callable=csv_file_exists,
        poke_interval=30,
        timeout=TIMEOUT,
        mode='poke'
    )

    process = PythonOperator(
        task_id='process_all_csvs',
        python_callable=process_file
    )

    archive = PythonOperator(
        task_id='archive_all_csvs',
        python_callable=archive_csvs
    )

    wait_for_any_csv >> process >> archive
