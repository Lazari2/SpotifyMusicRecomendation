from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'train_kmeans_model',
    default_args=default_args,
    description='DAG para treinar e versionar o modelo KMeans usando MLflow',
    schedule_interval='@weekly',  
)

script_path = '/opt/airflow/MachineLearning/KmeansML.py'

train_model_task = BashOperator(
    task_id='train_model_task',
    bash_command=f'python {script_path}',
    dag=dag,
)
