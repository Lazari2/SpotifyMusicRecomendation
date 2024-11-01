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
    'update_spotify_api_token',
    default_args=default_args,
    description='DAG para executar o script token_request.py a cada 45 minutos',
    schedule_interval='*/45 * * * *' 
)

script_path = '/opt/airflow/python_scripts/token_request.py'

run_extraction_script = BashOperator(
    task_id='run_token_update_script_task',
    bash_command=f'python {script_path}',
    dag=dag,
)