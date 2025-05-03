from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Chiamaka',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'dags_with_catchup_and_backfill_v1',
    default_args= default_args,
    description='This is our first dag',
    start_date=datetime(2025,4,30),
    schedule_interval= '@daily',
    catchup= False
) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command="echo This is a simple bash command!"
    )