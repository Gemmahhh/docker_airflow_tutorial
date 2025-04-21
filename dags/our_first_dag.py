from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Chiamaka',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'our_first_dag_v3',
    default_args= default_args,
    description='This is our first dag',
    start_date=datetime(2025,4,20,9),
    schedule_interval= '@daily'
) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command="echo hello world, this is the first task!"
    )
    task2 = BashOperator(
        task_id = 'second_task',
        bash_command="echo this is the second task which would be running after the first task"
    )
    task3 = BashOperator(
        task_id = 'third_task',
        bash_command="echo this is the third task which would be running after the first task at the same time with task 2"
    )
    task1.set_downstream(task2)
    task1.set_downstream(task3)