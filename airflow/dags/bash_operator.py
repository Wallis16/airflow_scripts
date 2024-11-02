from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore

default_args = {
    'owner': 'Isaac',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='bash_operator_dag_v3',
    default_args=default_args,
    description='Just a DAG using bash operators',
    start_date=datetime(2024, 9, 27, 2),
    schedule_interval='@daily'
) as dag:

    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo first task"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo second task"
    )

    task3 = BashOperator(
        task_id='thrid_task',
        bash_command="echo third task"
    )

    task1 >> [task2, task3]