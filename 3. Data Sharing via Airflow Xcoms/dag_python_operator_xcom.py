from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'bagushakim',
    'retry' : 5,
    'retry_delay' : timedelta(minutes=5)
}

def greet(name, age):
    print(f"Hello, my name is {name} "
          f"and i am {age} years old.")

# Share information between different tasks using airflow xcoms
def get_name():
    return 'Bagus'

with DAG (
    default_args = default_args,
    dag_id = 'dag_with_python_operator_v3',
    description = 'My first dag using python operator',
    start_date=datetime(2021, 10, 6),
    schedule_interval='@daily'
) as dag:

    task2 = PythonOperator(
        task_id = 'get_name',
        python_callable = get_name
    )

    task2