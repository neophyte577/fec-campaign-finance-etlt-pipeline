from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime, duration, now

import requests
import os
import shutil 
import zipfile
import polars as pl

name = 'committee_contributions'
cycle = '2024'
suffix = cycle[len(cycle)-2:]
fec_code = 'pas2'
run_date = 'today'
extension = '.csv'
output_name = f'{run_date}_{name}_{cycle}{extension}'

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": duration(seconds=30),
}

@dag(
    dag_id="fetch",
    start_date=datetime(2025, 1, 1),
    schedule=None,  
    catchup=False,
    is_paused_upon_creation=False
)

def ingest():
    run_date = now().to_date_string()

    temp_dir = '/opt/airflow/dags/temp/'

    input_dir = f'{temp_dir}{name}_{cycle}/in/'
    output_dir = f'{temp_dir}{name}_{cycle}/out/'

    header_dir = input_dir + 'header/'
    header_path = header_dir + f'{name}_header.csv'

    data_dir = input_dir + 'data/'

    cleaned_data_path = data_dir + 'cleaned_data.txt'

    @task
    def start():
        EmptyOperator(task_id="start")

    @task
    def create_dirs():
        for directory in [temp_dir, input_dir, output_dir, header_dir, data_dir]:
            if os.path.exists(directory):
                shutil.rmtree(directory)
            os.makedirs(directory)
                
    @task
    def get_header():
        header_url = f'https://www.fec.gov/files/bulk-downloads/data_dictionaries/{fec_code}_header_file.csv'
        
        with open(header_path, 'wb') as header:
            header.write(requests.get(header_url).content)

    @task
    def get_data():
        data_url = f'https://www.fec.gov/files/bulk-downloads/{cycle}/{fec_code}{suffix}.zip'
        
        zip_path = data_dir + f'{name}_{cycle}.zip'  

        with open(zip_path, 'wb') as zipped:  
            zipped.write(requests.get(data_url)._content)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(data_dir)

        os.remove(zip_path)
    
    @task
    def preprocess():

        raw_data_path = data_dir + os.listdir(data_dir)[0]

        with open(raw_data_path, 'r', encoding='utf-8') as file:
            cleaned = file.read().replace(',|', '|')

        with open(cleaned_data_path, 'w', encoding='utf-8') as cleaned_data:
            cleaned_data.write(cleaned)
    
    @task
    def write():

        column_names = pl.read_csv(header_path, has_header=False).row(0)

        dtype = {name : pl.Utf8 for name in column_names}

        df = pl.read_csv(cleaned_data_path, separator='|', new_columns=column_names, 
                        schema_overrides=dtype, infer_schema_length=0, encoding="utf-8")

        df.write_csv(output_dir + output_name, line_terminator='\n')

    trigger_staging = TriggerDagRunOperator(task_id='trigger_staging', trigger_dag_id='stage', wait_for_completion=False)

    @task
    def stop():
        EmptyOperator(task_id="stop")
    
    start() >> create_dirs() >> get_header() >> get_data() >> preprocess() >> write() >> trigger_staging >> stop()

ingest()



