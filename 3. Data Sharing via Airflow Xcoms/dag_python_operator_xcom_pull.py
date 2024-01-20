from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'bagushakim',
    'retry' : 5,
    'retry_delay' : timedelta(minutes=5)
}


def greet(age, ti): # ti mean task instance, xcom pull can only be called by ti
    name = ti.xcom_pull(task_ids='get_name')
    print(f"Hello, my name is {name} "
          f"and i am {age} years old.")

# Share information between different tasks using airflow xcoms
def get_name():
    return 'Bagus'

with DAG (
    default_args = default_args,
    dag_id = 'dag_with_python_operator_v4',
    description = 'My first dag using python operator',
    start_date=datetime(2021, 10, 6),
    schedule_interval='@daily'
) as dag:
    
    task1 = PythonOperator(
        task_id = 'greet',
        python_callable = greet,
        op_kwargs = {'age' : 20}
    )

    task2 = PythonOperator(
        task_id = 'get_name',
        python_callable = get_name
    )

    task2 >> task1