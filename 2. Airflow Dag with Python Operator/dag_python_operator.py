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

with DAG (
    default_args = default_args,
    dag_id = 'dag_with_python_operator_v2',
    description = 'My first dag using python operator',
    start_date=datetime(2021, 10, 6),
    schedule_interval='@daily'
) as dag:
    
    task1 = PythonOperator(
        task_id = 'greet',
        python_callable = greet,
        op_kwargs = {'name' : 'Bagus', 'age' : 23}
    )

    task1