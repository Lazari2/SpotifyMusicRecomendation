from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 20), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'update_spotify_track_ids',
    default_args=default_args,
    description='DAG para executar dataset_extraction.py de hora em hora',
    schedule_interval='@hourly', 
)

script_path = '/opt/airflow/python_scripts/dataset_extraction.py'

run_extraction_script = BashOperator(
    task_id='run_extraction_script_task',
    bash_command=f'python {script_path}',
    dag=dag,
)