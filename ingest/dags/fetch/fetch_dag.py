from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime, duration, now

import requests
import os, gc
import shutil 
import zipfile
import concurrent.futures
import polars as pl
import pandas as pd

DTYPE_MAPPING = {
    'VARCHAR': pl.Utf8,
    'STRING': pl.Utf8,
    'NUMBER': pl.Float64,
    'NUMERIC': pl.Float64,
    'INTEGER':pl.Int64,
    'INT': pl.Int64,
    'FLOAT': pl.Float64,
    'DECIMAL': pl.Float64,
    'BOOLEAN': pl.Boolean,
    'DATE': pl.Date,
    'TIMESTAMP': pl.Datetime,
    'TIME': pl.Time,
}

MB_THRESHOLD = 3072

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

    keys = ['name', 'fec_code', 'cycle', 'run_date', 'extension', 'temp_dir']

    @task
    def process_config(**context): 
        dag_run = context['dag_run'].conf
        config = {key: dag_run.get(key) for key in keys}
        return config

    @task
    def initialize_paths(config):
        name = config['name']
        fec_code = config['fec_code']
        cycle = config['cycle']
        run_date = config['run_date']
        extension = config['extension']
        temp_dir = config['temp_dir']

        suffix = cycle[-2:]
        
        input_dir = f'{temp_dir}{name}_{cycle}/in/'
        output_dir = f'{temp_dir}{name}_{cycle}/out/'
        cleaned_data_path = input_dir + 'cleaned_data.txt'
        output_name = f'{run_date}_{name}_{cycle}{extension}'

        paths = {
            'input_dir': input_dir,
            'output_dir': output_dir,
            'input_dir': input_dir,
            'cleaned_data_path': cleaned_data_path,
            'output_name': output_name,
            'cycle': cycle,
            'fec_code': fec_code,
            'suffix': suffix,
            'name': name
        }

        return paths

    @task
    def start():
        EmptyOperator(task_id="start")

    @task
    def create_dirs(paths):
        input_dir = paths['input_dir']
        output_dir = paths['output_dir']

        for directory in [input_dir, output_dir]:
            if os.path.exists(directory):
                shutil.rmtree(directory)
            os.makedirs(directory)

    @task
    def get_data(paths):
        name = paths['name']
        cycle = paths['cycle']
        fec_code = paths['fec_code']
        suffix = paths['suffix']
        input_dir = paths['input_dir']
        data_url = f'https://www.fec.gov/files/bulk-downloads/{cycle}/{fec_code}{suffix}.zip'
        
        zip_path = f'{input_dir}{name}_{cycle}.zip'  

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Cache-Control": "max-age=0",
            "Upgrade-Insecure-Requests": "1"
        }        

        with requests.get(data_url, headers=headers, stream=True) as response:
            response.raise_for_status() 
            with open(zip_path, 'wb') as zipped:
                for chunk in response.iter_content(chunk_size=1048576): 
                    if chunk:
                        zipped.write(chunk)
                    gc.collect()

    @task
    def extract_data(paths):
        name = paths['name']
        cycle = paths['cycle']
        input_dir = paths['input_dir']

        zip_path = f'{input_dir}{name}_{cycle}.zip'

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            data_files = [f for f in zip_ref.namelist() if "/" not in f.strip("/")]
            if data_files:
                for file in data_files:
                    zip_ref.extract(file, input_dir)

        # with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        #     zip_ref.extractall(input_dir)

        os.remove(zip_path)

    @task
    def preprocess(paths):
        input_dir = paths['input_dir']
        cleaned_data_path = paths['cleaned_data_path']
        raw_data_path = os.path.join(input_dir, os.listdir(input_dir)[0])

        file_size = os.path.getsize(raw_data_path)
        file_size_mb = file_size / (1024 * 1024)

        # condition approach on MG_THRESHOLD size to parse by row
        if file_size_mb < MB_THRESHOLD: # all at once
            with open(raw_data_path, 'r', encoding='utf-8') as file:
                cleaned = file.read().replace(',|', '|').replace('"','').replace("'","")
            with open(cleaned_data_path, 'w', encoding='utf-8') as cleaned_data:
                cleaned_data.write(cleaned)

        else: # in chunks, concurrently threaded
            chunk_size = 100000

            def clean_line(line):
                return line.replace(',|', '|').replace('"', '').replace("'", "")
            
            def process_chunk(chunk_lines, cleaned_data_path):
                with open(cleaned_data_path, 'a', encoding='utf-8') as cleaned_data:
                    for cleaned_line in map(clean_line, chunk_lines):
                        cleaned_data.write(cleaned_line + '\n')

            with open(raw_data_path, 'r', encoding='utf-8') as file:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    while True:
                        lines = file.readlines(chunk_size)
                        if not lines:
                            break
                        executor.submit(process_chunk, lines, cleaned_data_path)

        gc.collect()
    
    @task
    def write(paths):
        cleaned_data_path = paths['cleaned_data_path']
        output_name = paths['output_name']
        output_dir = paths['output_dir']

        def parse_data_type(data_type):
            data_type_upper = data_type.upper()
            if 'DATE' in data_type_upper:
                return pl.Utf8
            for sql_type in DTYPE_MAPPING:
                if sql_type in data_type_upper:
                    return DTYPE_MAPPING[sql_type]
            return pl.Utf8  # default to Utf8 

        schema_df = pd.read_csv(f'/opt/airflow/metadata/schemas/{paths["name"]}.csv')
        header = list(schema_df['attribute'])
        dtype = {row['attribute']: parse_data_type(row['data_type']) for index, row in schema_df.iterrows()}

        file_size = os.path.getsize(cleaned_data_path)
        file_size_mb = file_size / (1024 * 1024)

        def convert_date_columns(df, date_columns):
            for date_col in date_columns:
                if date_col in df.columns:
                    df = df.with_columns(pl.col(date_col).str.strptime(pl.Date, format="%m%d%Y", strict=False))

            return df
        
        date_columns = [row['attribute'] for index, row in schema_df.iterrows() if 'DATE' in row['data_type'].upper()]     

        # condition approach on MG_THRESHOLD size to parse by row
        if file_size_mb < MB_THRESHOLD:  # all at once

            df = pl.read_csv(cleaned_data_path, separator='|', new_columns=header, 
                            schema_overrides=dtype, infer_schema_length=0, encoding="utf-8", ignore_errors=True)

            df = convert_date_columns(df, date_columns) 
            df.write_parquet(os.path.join(output_dir, output_name))

        else:  # in chunks

            reader = pl.read_csv_batched(cleaned_data_path, separator='|', new_columns=header, 
                                        schema_overrides=dtype, infer_schema_length=0, encoding="utf-8", ignore_errors=True)

            batches = reader.next_batches(100000)
            while batches:
                df = pl.concat(batches)
                df = convert_date_columns(df, date_columns)  # Ensure conversion here
                df.write_parquet(os.path.join(output_dir, output_name), append=True)
                batches = reader.next_batches(100000)
                gc.collect()

    trigger_staging_task = TriggerDagRunOperator(
        task_id='trigger_staging',
        trigger_dag_id='stage', 
        conf = {key: f'{{{{ ti.xcom_pull(task_ids="process_config")["{key}"] }}}}' for key in keys},
        wait_for_completion=False  
    )

    @task
    def stop():
        EmptyOperator(task_id="stop")

    config = process_config()
    paths = initialize_paths(config)

    start() >> create_dirs(paths) >> get_data(paths) >> extract_data(paths) >> preprocess(paths) >> write(paths) >> trigger_staging_task >> stop()


ingest()



