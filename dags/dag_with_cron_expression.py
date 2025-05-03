from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Chiamaka',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'dag_with_cron_expression_v2',
    default_args= default_args,
    description='This is our first dag with cron expression',
    start_date=datetime(2025,5,1),
    schedule_interval= '0 3 * * 6,0,1',
    catchup= False
) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command="echo This is a dag with cron expression!"
    )