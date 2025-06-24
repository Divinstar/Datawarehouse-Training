from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import os

def check_file_size():
    size = os.path.getsize("data/inventory.csv")
    return "light_task" if size < 500_000 else "heavy_task"

def light_summary():
    print("Running light summary...")

def detailed_processing():
    print("Running detailed processing...")

def cleanup():
    print("Cleaning up...")

with DAG("branching_file_size_dag",
         start_date=datetime(2023, 1, 1),
         schedule_interval=None,
         catchup=False) as dag:

    branch = BranchPythonOperator(task_id="branch_decision", python_callable=check_file_size)
    light = PythonOperator(task_id="light_task", python_callable=light_summary)
    heavy = PythonOperator(task_id="heavy_task", python_callable=detailed_processing)
    join = DummyOperator(task_id="join", trigger_rule="none_failed_min_one_success")
    final_cleanup = PythonOperator(task_id="cleanup", python_callable=cleanup)

    branch >> [light, heavy] >> join >> final_cleanup