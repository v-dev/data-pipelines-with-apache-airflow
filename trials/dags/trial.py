from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2022, 5, 1)
}

with DAG("trial_dag", schedule_interval='0 0 * * *', default_args=default_args, catchup=False) as trial_dag:
    def say_hello():
        print("hi from say_hello python function")

    bash = BashOperator(
        task_id='bash',
        bash_command='echo hello world from BashOperator!'
    )

    py = PythonOperator(
        task_id="py",
        python_callable=say_hello
    )

    cat = BashOperator(
        task_id='cat',
        bash_command='cat /tmp/requirements.txt'
    )

    py >> bash
