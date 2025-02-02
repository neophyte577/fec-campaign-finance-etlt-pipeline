from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pendulum import today, duration, datetime
import pandas as pd

file_path = '/opt/airflow/dags/exploratory/2025-01-01_committee_contributions_2024.csv'

df = pd.read_csv(file_path)

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
      return 'VARCHAR(69)'  
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
    start_date=datetime(2025,1,1), 
    schedule='@daily', 
    catchup=False,
    default_args=default_args, 
    # params={"run_date": Param(today().to_date_string(), type="string")}
)
def load_committee_contributions():

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

        run_date = today().to_date_string()

        load_committee_contributions = f"""
        COPY INTO fec_db.raw.committee_contributions_2024
            FROM @FEC_DB.RAW.S3_STAGE/campaign-finance/2025-01-01_committee_contributions_2024.csv
            ON_ERROR = 'CONTINUE'
            FILE_FORMAT = my_csv_format
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """
        
        load_query = SnowflakeOperator(
            task_id="snowflake_query",
            sql=load_committee_contributions,
            snowflake_conn_id="snowflake_conn"
        )

        try:
            load_query.execute(context={})
        except Exception as e:
            print("Error executing Snowflake query:", e)
            raise 
    
    @task
    def stop():
        EmptyOperator(task_id="stop")

    start() >> truncate() >> create() >> load() >> stop()

load_committee_contributions()
