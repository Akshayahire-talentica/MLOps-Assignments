#!/usr/bin/env python3
"""
Simple MLOps Pipeline DAG for Airflow
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'mlops',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mlops_pipeline',
    default_args=default_args,
    description='Basic MLOps pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['mlops'],
)

def hello_mlops():
    print("Hello from MLOps Pipeline!")
    return "Success"

# Simple task to verify Airflow is working
hello_task = PythonOperator(
    task_id='hello_mlops',
    python_callable=hello_mlops,
    dag=dag,
)
