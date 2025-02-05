from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from pendulum import today, duration, datetime
import os
import shutil
import pandas as pd

def sql_query(df, table_name):
  sql = f"CREATE OR REPLACE TABLE {table_name} (\n"
  for column in df.columns:
    data_type = get_snowflake_data_type(df[column].dtype) 
    sql += f"    {column} {data_type},\n"
  sql = sql[:-2] + ");" 
  return sql

def get_snowflake_data_type(pandas_dtype):
  if pandas_dtype == 'int64':
      return 'INT'
  elif pandas_dtype == 'float64':
      return 'FLOAT'
  elif pandas_dtype == 'object': 
      return 'VARCHAR(420)'  
  else:
      return 'VARCHAR(420)' 

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
    is_paused_upon_creation=False,
    # params={"run_date": Param(today().to_date_string(), type="string")}
)
def load_data():

    keys = ['name', 'fec_code', 'cycle', 'run_date', 'extension', 'temp_dir']

    @task
    def process_config(**context):
        dag_run = context['dag_run'].conf
        config = {'name': dag_run.get('name'), 'fec_code': dag_run.get('fec_code'), 'cycle': dag_run.get('cycle'), 
                  'run_date': dag_run.get('run_date'), 'extension': dag_run.get('extension'), 'temp_dir': dag_run.get('temp_dir')}
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
        header_path = f'{temp_dir}{name}_{cycle}/in/header/{name}_header.csv'

        paths = {
            'output_name': output_name,
            'cycle': cycle,
            'fec_code': fec_code,
            'suffix': suffix,
            'run_date': run_date,
            'name': name,
            'table_name': table_name,
            'file_path': file_path,
            'header_path': header_path
        }

        return paths
    
    @task(outlets=['df'])
    def initialize_df(paths):

        header = pd.read_csv(paths['header_path'])

        df = pd.read_csv(paths['file_path'], low_memory=False)

        extra = list(set(df.columns) - set(list(header))) 

        df.drop(extra, axis='columns', inplace=True)

        return df

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
    def create_table(paths, df):

        create_table = sql_query(df, paths['table_name'])

        create_query = SnowflakeOperator(
            task_id="create_query",
            sql=create_table,
            snowflake_conn_id="snowflake_conn"
        )

        create_query.execute(context={})

    @task
    def load(paths, df):

        load_data = f"""
        COPY INTO fec_db.raw.{paths['table_name']} {str(tuple(df.columns)).replace("'","")}
            FROM @FEC_DB.RAW.S3_STAGE/campaign-finance/{paths['output_name']}
            FILE_FORMAT = my_csv_format
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
    def clean_up():
        # shutil.rmtree(f'/opt/airflow/dags/temp/{name}_{cycle}')
        pass

    @task
    def stop():
        EmptyOperator(task_id="stop")

    config = process_config()
    paths = initialize_paths(config)
    df = initialize_df(paths)

    start() >> truncate(paths) >> create_table(paths, df) >> load(paths, df) >> clean_up() >> stop()

load_data()
