from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args ={
    'owner': 'Chiamaka',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids = 'get_name_and_age', key = 'first_name')
    last_name = ti.xcom_pull(task_ids = 'get_name_and_age', key = 'last_name')
    age = ti.xcom_pull(task_ids = 'get_name_and_age', key = 'age')
    print(f"Hello World! My name is {first_name} {last_name}, and I am {age} years old")

def get_name_and_age(ti):
    ti.xcom_push(key = 'first_name', value = 'Chiamaka')
    ti.xcom_push(key = 'last_name', value = 'Okpe')
    ti.xcom_push(key = 'age', value = 25)

with DAG(
    default_args= default_args,
    dag_id = 'our_dag_with_python_operator_v06',
    description= 'Our first dag using Python Operator',
    start_date= datetime(2025,4, 20),
    schedule_interval= '@daily'
) as dag:
    task1 = PythonOperator(
        task_id = 'greet',
        python_callable= greet
    )
    task2 = PythonOperator(
        task_id = 'get_name_and_age',
        python_callable= get_name_and_age
    )
    # this means that task 1 will run after task 2 has been completed. 
    task2 >> task1