from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
import pandas as pd

def check_file():
    if not os.path.exists('data/customers.csv'):
        raise FileNotFoundError("File data/customers.csv not found")

def count_rows():
    df = pd.read_csv('data/customers.csv')
    row_count = len(df)
    print(f"Total rows: {row_count}")
    return row_count

with DAG(dag_id="csv_summary_dag",
         start_date=datetime(2023, 1, 1),
         schedule_interval=None,
         catchup=False) as dag:

    t1 = PythonOperator(task_id="check_file", python_callable=check_file)
    t2 = PythonOperator(task_id="count_rows", python_callable=count_rows)
    t3 = BashOperator(task_id="notify_if_large",
                      bash_command="echo 'More than 100 rows!'",
                      trigger_rule="all_done")

    t1 >> t2 >> t3