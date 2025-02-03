from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from pendulum import today, duration, datetime
import shutil
import pandas as pd

name = 'committee_contributions'
cycle = '2024'
suffix = cycle[len(cycle)-2:]
fec_code = 'pas2'
run_date = 'today'
extension = '.csv'
output_name = f'{run_date}_{name}_{cycle}{extension}'

file_path = f'/opt/airflow/dags/temp/{name}_{cycle}/out/{output_name}'

df = pd.read_csv(file_path, low_memory=False)

table_name = 'committee_contributions_2024'

def sql_query(df, table_name):
  sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
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
    "retry_delay": duration(seconds=30),
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

    @task
    def start():
        EmptyOperator(task_id="start")

    @task
    def truncate():

        truncate_committee_contributions = "TRUNCATE TABLE IF EXISTS fec_db.raw.committee_contributions_2024;"

        truncate_query = SnowflakeOperator(
            task_id="snowflake_query",
            sql=truncate_committee_contributions,
            snowflake_conn_id="snowflake_conn"
        )

        truncate_query.execute(context={})

    @task
    def create():

        create_committee_contributions = sql_query(df, table_name)

        create_query = SnowflakeOperator(
            task_id="create_query",
            sql=create_committee_contributions,
            snowflake_conn_id="snowflake_conn"
        )

        create_query.execute(context={})

    @task
    def load():

        load_data = f"""
        COPY INTO fec_db.raw.committee_contributions_2024
            FROM @FEC_DB.RAW.S3_STAGE/campaign-finance/2025-01-01_committee_contributions_2024.csv
            ON_ERROR = 'CONTINUE'
            FILE_FORMAT = my_csv_format
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
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

    start() >> truncate() >> create() >> load() >> clean_up() >> stop()

load_data()
