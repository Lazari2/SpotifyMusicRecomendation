from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),  
    'retries': 1,
}

dag = DAG(
    'CACHOOOOOOOOOOORRROOOOOOOOOOOOOOO',
    default_args=default_args,
    description='Uma DAG simples de exemplo',
    schedule_interval='@daily',  
)

def hello_world():
    print("Hello, world!")

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=hello_world,
    dag=dag,
)