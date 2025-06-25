from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os
import logging

FILE_PATH = '/opt/airflow/data/input/sales.csv'
REQUIRED_COLUMNS = ['order_id', 'product', 'quantity', 'price']

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

def validate_data():
    if not os.path.exists(FILE_PATH):
        raise FileNotFoundError("sales.csv not found.")
    df = pd.read_csv(FILE_PATH)
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
        if df[col].isnull().any():
            raise ValueError(f"Column '{col}' contains nulls.")
    logging.info("Validation passed.")

def summarize():
    df = pd.read_csv(FILE_PATH)
    logging.info(f"Summary:\n{df.describe()}")

with DAG(
    dag_id='data_quality_checker',
    default_args=default_args,
    description='Validate CSV file before processing',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    validate = PythonOperator(
        task_id='validate_sales_data',
        python_callable=validate_data
    )

    summarize_data = PythonOperator(
        task_id='summarize_data',
        python_callable=summarize
    )

    validate >> summarize_data