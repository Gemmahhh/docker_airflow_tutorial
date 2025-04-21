from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args ={
    'owner': 'Chiamaka',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def greet():
    print("Hello World")

with DAG(
    default_args= default_args,
    dag_id = 'our_dag_with_python_operator_v01',
    description= 'Our first dag using Python Operator',
    start_date= datetime(2025,4, 20),
    schedule_interval= '@daily'
) as dag:
    task1 = PythonOperator(
        task_id = 'greet',
        python_callable= greet
    )
    task1