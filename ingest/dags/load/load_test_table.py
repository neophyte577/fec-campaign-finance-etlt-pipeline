from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pendulum import today, duration, datetime

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
def load_test_table():

    @task
    def start():
        EmptyOperator(task_id="start")

    @task
    def truncate():

        truncate_test_table = "TRUNCATE TABLE IF EXISTS fec_db.raw.test_table;"

        snowflake_query = SnowflakeOperator(
            task_id="snowflake_query",
            sql=truncate_test_table,
            snowflake_conn_id="snowflake_conn"
        )

        snowflake_query.execute(context={})

    @task
    def create():

        create_test_table = '''
        CREATE OR REPLACE TABLE fec_db.raw.test_table (
            lorem VARCHAR(10),
            dolor VARCHAR(10),
            null_col VARCHAR(10),
            another_null VARCHAR(10),
            null_again VARCHAR(10),
            sic VARCHAR(10)
        );
        '''

        create_query = SnowflakeOperator(
            task_id="create_query",
            sql=create_test_table,
            snowflake_conn_id="snowflake_conn"
        )

        create_query.execute(context={})


    @task
    def load():
        
        load_test_table = f"""
        COPY INTO fec_db.raw.test_table
            FROM @FEC_DB.RAW.S3_STAGE/test_dir/test_csv.csv
            ON_ERROR = 'SKIP_FILE'
            FILE_FORMAT = (TYPE = CSV, PARSE_HEADER = TRUE)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """
        
        load_query = SnowflakeOperator(
            task_id="snowflake_query",
            sql=load_test_table,
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

load_test_table()
