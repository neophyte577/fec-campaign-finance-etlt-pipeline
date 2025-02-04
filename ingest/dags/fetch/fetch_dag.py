from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime, duration, now

import requests
import os
import shutil 
import zipfile
import polars as pl

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
    default_args=default_args,
    schedule_interval=None,  
    catchup=False,
    is_paused_upon_creation=False,
)
def ingest():

    @task
    def process_config(**context):
        dag_run = context['dag_run'].conf
        config = {'name': dag_run.get('dataset_name'), 'fec_code': dag_run.get('fec_code')}
        return config

    @task
    def initialize_paths(config):
        name = config['name']
        fec_code = config['fec_code']
        
        run_date = "today"
        extension = ".csv"
        cycle = "2024"
        suffix = cycle[-2:]
        temp_dir = '/opt/airflow/dags/temp/'
        
        input_dir = f'{temp_dir}{name}_{cycle}/in/'
        output_dir = f'{temp_dir}{name}_{cycle}/out/'
        header_dir = input_dir + 'header/'
        header_path = header_dir + f'{name}_header.csv'
        data_dir = input_dir + 'data/'
        cleaned_data_path = data_dir + 'cleaned_data.txt'
        output_name = f'{run_date}_{name}_{cycle}{extension}'

        return {
            'input_dir': input_dir,
            'output_dir': output_dir,
            'header_path': header_path,
            'data_dir': data_dir,
            'cleaned_data_path': cleaned_data_path,
            'output_name': output_name,
            'cycle': cycle,
            'fec_code': fec_code,
            'suffix': suffix,
            'name': name
        }

    @task
    def start():
        EmptyOperator(task_id="start")

    @task
    def create_dirs(paths):
        input_dir = paths['input_dir']
        output_dir = paths['output_dir']
        header_dir = os.path.dirname(paths['header_path'])
        data_dir = paths['data_dir']

        for directory in [input_dir, output_dir, header_dir, data_dir]:
            if os.path.exists(directory):
                shutil.rmtree(directory)
            os.makedirs(directory)
                
    @task
    def get_header(paths):
        header_path = paths['header_path']
        fec_code = paths['fec_code']
        header_url = f'https://www.fec.gov/files/bulk-downloads/data_dictionaries/{fec_code}_header_file.csv'
        
        with open(header_path, 'wb') as header:
            header.write(requests.get(header_url).content)

    @task
    def get_data(paths):
        name = paths['name']
        cycle = paths['cycle']
        fec_code = paths['fec_code']
        suffix = paths['suffix']
        data_dir = paths['data_dir']
        data_url = f'https://www.fec.gov/files/bulk-downloads/{cycle}/{fec_code}{suffix}.zip'
        
        zip_path = f'{data_dir}{name}_{cycle}.zip'  

        with open(zip_path, 'wb') as zipped:  
            zipped.write(requests.get(data_url).content)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(data_dir)

        os.remove(zip_path)
    
    @task
    def preprocess(paths):
        data_dir = paths['data_dir']
        cleaned_data_path = paths['cleaned_data_path']

        raw_data_path = os.path.join(data_dir, os.listdir(data_dir)[0])

        with open(raw_data_path, 'r', encoding='utf-8') as file:
            cleaned = file.read().replace(',|', '|')

        with open(cleaned_data_path, 'w', encoding='utf-8') as cleaned_data:
            cleaned_data.write(cleaned)
    
    @task
    def write(paths):
        header_path = paths['header_path']
        cleaned_data_path = paths['cleaned_data_path']
        output_name = paths['output_name']
        output_dir = paths['output_dir']

        column_names = pl.read_csv(header_path, has_header=False).row(0)

        dtype = {name : pl.Utf8 for name in column_names}

        df = pl.read_csv(cleaned_data_path, separator='|', new_columns=column_names, 
                        schema_overrides=dtype, infer_schema_length=0, encoding="utf-8")

        df.write_csv(os.path.join(output_dir, output_name), line_terminator='\n')

    def trigger_staging(config):
        return TriggerDagRunOperator(
        task_id='trigger_staging',
        trigger_dag_id='stage', 
        conf={
            'dataset_name': '{{ ti.xcom_pull(task_ids="process_config")["name"] }}',
            'fec_code': '{{ ti.xcom_pull(task_ids="process_config")["fec_code"] }}'
        },
        wait_for_completion=False  
    )

    @task
    def stop():
        EmptyOperator(task_id="stop")

    config = process_config()
    paths = initialize_paths(config)
    trigger_staging_task = trigger_staging(config)

    start() >> create_dirs(paths) >> get_header(paths) >> get_data(paths) >> preprocess(paths) >> write(paths) >> trigger_staging_task >> stop()


ingest()



