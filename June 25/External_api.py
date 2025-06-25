from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
import json

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

def parse_response(**kwargs):
    response_text = kwargs['ti'].xcom_pull(task_ids='call_api')
    data = json.loads(response_text)
    logging.info(f"Parsed API Response: {data}")

with DAG(
    dag_id='assignment7_external_api',
    default_args=default_args,
    description='Interact with a public API and parse the response',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    call_api = SimpleHttpOperator(
        task_id='call_api',
        http_conn_id='http_default',
        endpoint='v1/bpi/currentprice.json',
        method='GET',
        response_filter=lambda response: response.text,
        log_response=True
    )

    parse_api = PythonOperator(
        task_id='parse_api_response',
        python_callable=parse_response,
        provide_context=True
    )

    call_api >> parse_api