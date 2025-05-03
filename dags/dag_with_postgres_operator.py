from datetime import datetime, timedelta
from airflow import DAG 
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
#from airflow.providers.sql.operators.sql import SQLExecuteQueryOperator

default_args ={
    'owner': 'Chiamaka',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="dag_with_postgres_operator_v7",
    default_args = default_args,
    start_date= datetime(2025,5,2),
    schedule_interval="0 0 * * *"
) as dag:
    task1 = SQLExecuteQueryOperator(
        task_id="create_postgres_table",
        conn_id="postgres_localhost",
        sql = """
            create table if not exists dag_runs(
                dt date,
                dag_id character varying,
                primary key(dt, dag_id)
            )
        """
    )

    task2 = SQLExecuteQueryOperator(
        task_id="insert_into_table",
        conn_id="postgres_localhost",
        sql="""
            insert into dag_runs(dt, dag_id) values ('{{ds}}', '{{dag.dag_id}}')
        """
    )

    task3 = SQLExecuteQueryOperator(
        task_id="delete_from_table",
        conn_id="postgres_localhost",
        sql="""
            delete from dag_runs where dt = '{{ds}}' and dag_id = '{{dag.dag_id}}'
        """
    )

    task1 >> task3 >> task2