from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('hospital_data_download_dag', default_args=default_args, schedule_interval='@once') as dag:
    download_task = BashOperator(
        task_id='download_hospital_data',
        bash_command='python3 /opt/airflow/dags/Fetch-Hospital-Files.py',
    )

