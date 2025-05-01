from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args ={
    'owner': 'Chiamaka',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id = 'dag_with_taskflow_v1', 
    default_args = default_args,
    description= 'Our first dag using Python Operator',
    start_date= datetime(2025,4, 20),
    schedule_interval= '@daily')
def hello_world_etl():

    @task(multiple_outputs = True)
    def get_name():
        return {
            'firstname': 'Chiamaka',
            'lastname': 'Okpe'
        }
           
    @task()
    def get_age():
        return 25

    @task()
    def greet(firstname, lastname, age):
        print(f'Hello world, My name is {firstname} {lastname} and I am {age}')

    name_dict = get_name()
    age = get_age()
    greet(firstname = name_dict['firstname'], 
         lastname = name_dict['lastname'], 
         age = age)

greet_dag = hello_world_etl()