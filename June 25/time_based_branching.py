from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import datetime

def choose_branch():
    now = datetime.datetime.now()
    if now.weekday() >= 5:
        return 'skip_dag'
    elif now.hour < 12:
        return 'task_morning'
    else:
        return 'task_afternoon'

def print_msg(msg):
    print(msg)

with DAG(
    dag_id='Time_based_branch',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    decide = BranchPythonOperator(
        task_id='decide_task',
        python_callable=choose_branch
    )

    morning = PythonOperator(
        task_id='task_morning',
        python_callable=print_msg,
        op_args=['Good morning']
    )

    afternoon = PythonOperator(
        task_id='task_afternoon',
        python_callable=print_msg,
        op_args=['Good afternoon']
    )

    skip = EmptyOperator(task_id='skip_dag')

    end = EmptyOperator(task_id='final_cleanup', trigger_rule='none_failed_min_one_success')

    decide >> [morning, afternoon, skip] >> end