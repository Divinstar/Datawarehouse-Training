from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

def child_task(**kwargs):
    conf = kwargs.get('dag_run').conf
    logging.info(f"Child DAG triggered with: {conf}")

with DAG(
    dag_id='dag_trigger',
    start_date=days_ago(1),
    schedule_interval=None, 
    catchup=False,
) as child_dag:

    task = PythonOperator(
        task_id='child_task',
        python_callable=child_task,
        provide_context=True
    )