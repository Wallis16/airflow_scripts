from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore

default_args = {
    'owner': 'Newton',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def greet(**kwargs):
    user_name = kwargs['ti'].xcom_pull(task_ids='get_name')
    user_power = kwargs['ti'].xcom_pull(task_ids='get_power')
    
    first_name = user_name['first_name']
    power = user_power['power']
    print(f"The user is {first_name} with power {power}")

def get_name(name):
    user_name = {'first_name': name['user_real']}
    return user_name

def get_power():
    user_power = {'power': 100}
    return user_power

with DAG(
    dag_id='python_operator_dag_v1',
    default_args=default_args,
    description='Just a DAG using python operators',
    start_date=datetime(2024, 10, 10, 2),
    schedule_interval='@daily'
) as dag:
        
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        provide_context=True
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name,
        op_kwargs={'name': {'user_real': 'Xavi'}}
    )

    task3 = PythonOperator(
        task_id='get_power',
        python_callable=get_power
    )

    [task2, task3] >> task1
