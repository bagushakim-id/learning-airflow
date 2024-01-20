from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# define the common parameters to initialise the operator in default args
default_args = {
    'owner': 'bts-bagushakim',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG (
    dag_id = 'my_third_dag', # has to be made of alphanumeric characters, dashes, dots and underscores exclusively
    default_args = default_args,
    description = 'This is my first DAG uhuy \"_"/',
    # decide when we want to start the DAG and how often to execute it
    start_date = datetime(2023, 1, 1, 2), # 2 disitu artinya pukul 2 AM
    schedule_interval = '@daily'
) as dag:
    task_1 = BashOperator(
        task_id = 'first_task',
        bash_command = "echo hello world, this is the first task!!"
    )

    task_2 = BashOperator(
        task_id = 'second_task',
        bash_command = "echo hellow, this is me the second task after the first task"
    )

    task_3 = BashOperator(
        task_id = 'third_task',
        bash_command = "echo hi im third task, i will be running at the same time with task 2 after task 1 finish"
    )

    # # Task depedency method 1
    # task_1.set_downstream(task_2)
    # task_1.set_downstream(task_3)

    # # Task depedency method 2
    # task_1 >> task_2
    # task_1 >> task_3

    # Task depedency method 3
    task_1 >> [task_2, task_3]

# Task
# a task is an implementation of an operator