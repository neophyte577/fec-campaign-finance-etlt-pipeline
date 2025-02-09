from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from pendulum import duration, datetime
import os
import shutil
import pandas as pd

def get_schema_dict():
    schema_dir = '/opt/airflow/schemas/'
    schemas = [file for file in os.listdir(schema_dir) if file.endswith('.csv')]
    schema_dict = {}
    for file_name in schemas:
        dataset_name = file_name[:-4]
        schema_dict[dataset_name] = pd.read_csv(schema_dir + file_name)
    
    return schema_dict

def sql_query(schema_df, table_name):

  sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
  for attribute in schema_df['attribute']:
    data_type = schema_df.loc[schema_df['attribute'] == attribute, 'data_type'].values[0]
    sql += f"    {attribute} {data_type},\n"
  sql = sql[:-2] + ");" 
  return sql

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": duration(seconds=30)
}

@dag(
    dag_id='load_data',
    schedule_interval=None,
    start_date=datetime(2025,1,1), 
    schedule=None,
    catchup=False,
    default_args=default_args, 
    is_paused_upon_creation=False
)
def load_data():

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
        temp_dir = '/opt/airflow/dags/temp/'
        
        output_name = f'{run_date}_{name}_{cycle}{extension}'
        table_name = f'{name}_{cycle}'
        file_path = f'{temp_dir}{name}_{cycle}/out/{output_name}'

        paths = {
            'output_name': output_name,
            'cycle': cycle,
            'fec_code': fec_code,
            'suffix': suffix,
            'run_date': run_date,
            'name': name,
            'table_name': table_name,
            'file_path': file_path,
        }

        return paths

    @task
    def start():
        EmptyOperator(task_id="start")

    @task
    def truncate(paths):

        truncate_table = f"TRUNCATE TABLE IF EXISTS fec_db.raw.{paths['table_name']};"

        truncate_query = SnowflakeOperator(
            task_id="snowflake_query",
            sql=truncate_table,
            snowflake_conn_id="snowflake_conn"
        )

        truncate_query.execute(context={})

    @task
    def create_table(paths, schema_dict):

        schema_df = schema_dict[paths['name']]

        create_table = sql_query(schema_df, paths['table_name'])

        create_query = SnowflakeOperator(
            task_id="create_query",
            sql=create_table,
            snowflake_conn_id="snowflake_conn"
        )

        create_query.execute(context={})

    @task
    def load(paths):

        load_data = f"""
        COPY INTO fec_db.raw.{paths['table_name']}
            FROM @FEC_DB.RAW.S3_STAGE/campaign-finance/{paths['output_name']}
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE';
        """
        
        load_query = SnowflakeOperator(
            task_id="snowflake_query",
            sql=load_data,
            snowflake_conn_id="snowflake_conn"
        )

        try:
            load_query.execute(context={})
        except Exception as e:
            print("Error executing Snowflake query:", e)
            raise 
    
    @task
    def clean_up(paths):
        name, cycle = paths['name'], paths['cycle']
        shutil.rmtree(f'/opt/airflow/dags/temp/{name}_{cycle}')
        pass

    @task
    def stop():
        EmptyOperator(task_id="stop")

    config = process_config()
    paths = initialize_paths(config)
    schema_dict = get_schema_dict()

    start() >> truncate(paths) >> create_table(paths, schema_dict) >> load(paths) >> clean_up(paths) >> stop()

load_data()
