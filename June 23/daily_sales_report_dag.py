from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import os
import shutil

def read_and_summarize():
    df = pd.read_csv("data/sales.csv")
    summary = df.groupby("category")["amount"].sum().reset_index()
    summary.to_csv("data/sales_summary.csv", index=False)

def archive_file():
    shutil.move("data/sales.csv", "data/archive/sales.csv")

with DAG("daily_sales_report",
         schedule_interval="0 6 * * *",
         start_date=days_ago(1),
         dagrun_timeout=timedelta(minutes=5),
         catchup=False) as dag:

    summarize = PythonOperator(task_id="summarize_sales", python_callable=read_and_summarize)
    archive = PythonOperator(task_id="archive_sales", python_callable=archive_file)

    summarize >> archive