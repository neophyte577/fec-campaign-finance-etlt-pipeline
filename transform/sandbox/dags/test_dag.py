from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
import os

SOURCE_FILE_PATH = '/opt/airflow/dbt/transformations/models/marts/generic_tests.yml'
TARGET_DIR = '/opt/airflow/dbt_staging'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay':0,
}

with DAG('test_dag',
        default_args=default_args,
        schedule_interval='@daily',
        start_date=days_ago(1),
        catchup=False) as dag:
    
    start = EmptyOperator(task_id="start")
    
    saved = EmptyOperator(task_id="saved")

    test = BashOperator(
        task_id='test',
        bash_command='cd /opt/airflow/dbt/transformations && dbt run',
    )

    @task
    def log_manual_trigger():
        print("Please execute the 'save_to_deploy' task manually via the Airflow UI if you want to push changes to production.")

    copy = BashOperator(
        task_id='copy_file',
        bash_command=f'bash /opt/airflow/scripts/write_to_deploy.sh {SOURCE_FILE_PATH} {TARGET_DIR}',
        dag=dag,
    )


    start >> test >> log_manual_trigger() >> copy >> saved



