from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore

default_args = {
    'owner': 'Gauss',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

def last(ti):

    name = ti.xcom_pull(task_ids='get_value2', key='value2')
    start = ti.xcom_pull(task_ids='get_value3', key='value3')
    end = ti.xcom_pull(task_ids='get_value4', key='value4')

    ti.xcom_push(key='last', value=start*end)

    print(f"Ops {name}, {start} and {end}")

def get_v2(ti):
    ti.xcom_push(key='value2', value='Dumont')

def get_v3(ti):
    ti.xcom_push(key='value3', value=1642)

def get_v4(ti):
    ti.xcom_push(key='value4', value=1727)

def get_v5(ti):
    last = ti.xcom_pull(task_ids='last_', key='last')
    ti.xcom_push(key='value5', value=10*last)

def get_v6(ti):
    v5 = ti.xcom_pull(task_ids='get_value5', key='value5')
    print(v5*100)

with DAG(
    dag_id='xcoms_dag_v1',
    default_args=default_args,
    description='Just a DAG using xcoms',
    start_date=datetime(2024, 10, 10, 2),
    schedule_interval='@daily'
) as dag:
    
    task1 = PythonOperator(
        task_id='last_',
        python_callable=last
    )

    task2 = PythonOperator(
        task_id='get_value2',
        python_callable=get_v2
    )

    task3 = PythonOperator(
        task_id='get_value3',
        python_callable=get_v3
    )

    task4 = PythonOperator(
        task_id='get_value4',
        python_callable=get_v4
    )

    task5 = PythonOperator(
        task_id='get_value5',
        python_callable=get_v5
    )

    task6 = PythonOperator(
        task_id='get_value6',
        python_callable=get_v6
    )

    [task2, task3, task4] >> task1 >> task5 >> task6