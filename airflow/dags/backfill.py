from datetime import datetime, timedelta

from airflow.decorators import dag, task # type: ignore

default_args = {
    'owner': 'Andrej',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

@dag(dag_id='backfill_dag_v1',
    default_args=default_args,
    description='Just a DAG using taskflow',
    start_date=datetime(2024, 10, 10, 2),
    schedule_interval='@daily',
    catchup=False)

def etl():

    @task(multiple_outputs=True)
    def get_infos():
        return {
            'id': 'Robert',
            'value': 100
        }

    @task()
    def get_size():
        return 30

    @task()
    def show(name, value, size):
        print(f"-- {name} {value} "
              f"-- {size} --")
    
    dict_ = get_infos()
    size = get_size()

    show(name=dict_['id'], 
          value=dict_['value'],
          size=size)

greet_dag = etl()