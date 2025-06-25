from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import time
import logging

def long_task():
    time.sleep(15)
    logging.info("Simulated long task completed.")

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    dag_id='assignment4_retry_timeout',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id='long_running_task',
        python_callable=long_task,
        execution_timeout=timedelta(seconds=20),  
    )
