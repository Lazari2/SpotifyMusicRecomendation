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
    'spotify_data_pipeline',
    default_args=default_args,
    description='Pipeline que atualiza o token, extrai IDs de faixas e coleta features de áudio',
    schedule_interval='@hourly',
)

update_token_task = BashOperator(
    task_id='run_token_update_script_task',
    bash_command='python /opt/airflow/python_scripts/token_request.py',
    dag=dag,
)

extract_track_ids_task = BashOperator(
    task_id='run_extraction_script_task',
    bash_command='python /opt/airflow/python_scripts/dataset_extraction.py',
    dag=dag,
)

fetch_audio_features_task = BashOperator(
    task_id='run_fetch_audio_script_task',
    bash_command='python /opt/airflow/python_scripts/fetch_audio_features.py',
    dag=dag,
)

update_token_task >> extract_track_ids_task >> fetch_audio_features_task
