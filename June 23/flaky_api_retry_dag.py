from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random

def flaky_api():
    if random.random() < 0.7:
        raise Exception("API failed")
    print("API call succeeded")

def success_task():
    print("Only runs if API succeeded.")

def alert_failure(context):
    print("ALERT: API failed after all retries.")

with DAG("flaky_api_retry_dag",
         start_date=datetime(2023, 1, 1),
         schedule_interval=None,
         catchup=False) as dag:

    api_call = PythonOperator(
        task_id="flaky_api_call",
        python_callable=flaky_api,
        retries=3,
        retry_exponential_backoff=True,
        on_failure_callback=alert_failure
    )

    success = PythonOperator(task_id="success_task", python_callable=success_task)
    api_call >> success