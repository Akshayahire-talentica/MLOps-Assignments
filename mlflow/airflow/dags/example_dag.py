from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_world",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    BashOperator(
        task_id="say_hi",
        bash_command="echo hello from airflow",
    )