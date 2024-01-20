from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task

default_args = {
    'owner' : 'bts-hakim',
    'retry' : 5,
    'retry-delay' : timedelta(minutes=5)
}

@dag(
        dag_id='dag_taskflow_api',
        default_args=default_args,
        start_date=datetime(2021, 10, 6),
        schedule_interval='@daily'
)
def hellow_world_etl():
    
    @task()
    def get_name():
        return "Bagus"
    
    @task()
    def get_age():
        return 18
    
    @task()
    def greet(name, age):
        print(f"Hellow World, My name is {name} and "
              f"I am {age} years old")

    name = get_name()
    age = get_age()
    greet(name=name, age=age)

greet_dag = hellow_world_etl()