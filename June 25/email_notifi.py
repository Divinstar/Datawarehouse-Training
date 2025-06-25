from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

default_args = {
    'owner': 'airflow',
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['your-email@gmail.com'], 
    'retries': 0,
    'retry_delay': timedelta(seconds=5),
}

def success_task():
    logging.info("Task succeeded.")

def fail_task():
    raise Exception("Intentional failure to trigger email.")

with DAG(
    dag_id='assignment6_email_notifications',
    default_args=default_args,
    description='Send email alerts based on task outcomes',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id='dummy_success',
        python_callable=success_task
    )

    task2 = PythonOperator(
        task_id='dummy_fail',
        python_callable=fail_task
    )

    task1 >> task2