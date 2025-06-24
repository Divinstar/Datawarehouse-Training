from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd

@task
def list_files():
    return [f for f in os.listdir("data/input") if f.endswith(".csv")]

@task
def process_file(filename: str):
    df = pd.read_csv(f"data/input/{filename}")
    if not {"id", "name"}.issubset(df.columns):
        raise ValueError(f"{filename} missing headers")
    row_count = len(df)
    summary = pd.DataFrame([[filename, row_count]], columns=["file", "row_count"])
    summary.to_csv(f"data/output/summary_{filename}", index=False)
    return f"summary_{filename}"

@task
def merge_summaries(files):
    dfs = [pd.read_csv(f"data/output/{f}") for f in files]
    final = pd.concat(dfs)
    final.to_csv("data/output/final_summary.csv", index=False)

with DAG("dynamic_csv_processor_dag",
         start_date=datetime(2023, 1, 1),
         schedule_interval=None,
         catchup=False) as dag:

    files = list_files()
    summaries = process_file.expand(filename=files)
    merge_summaries(summaries)