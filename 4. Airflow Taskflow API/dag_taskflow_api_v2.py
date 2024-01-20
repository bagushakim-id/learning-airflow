from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task

default_args = {
    'owner' : 'bts-hakim',
    'retry' : 5,
    'retry-delay' : timedelta(minutes=5)
}

@dag(
        dag_id='dag_taskflow_api_v2',
        default_args=default_args,
        start_date=datetime(2021, 10, 6),
        schedule_interval='@daily'
)
def hellow_world_etl():
    
    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name' : 'Bagus',
            'last_name' : 'Hakim'
        }
    
    @task()
    def get_age():
        return 18
    
    @task()
    def greet(first_name, last_name, age): 
        print(f"Hellow World, My name is {first_name} {last_name} "
              f"I am {age} years old")

    name_dict = get_name()
    age = get_age()
    greet(first_name = name_dict['first_name'],
          last_name = name_dict['last_name'],
          age=age)

greet_dag = hellow_world_etl()