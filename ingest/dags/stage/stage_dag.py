from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

name = 'committee_contributions'
cycle = '2024'
suffix = cycle[len(cycle)-2:]
fec_code = 'pas2'
run_date = 'today'
extension = '.csv'
output_name = f'{run_date}_{name}_{cycle}{extension}'
s3_dir = 'campaign-finance'

@dag(
    dag_id="stage",
    schedule_interval=None,
    start_date=datetime(2023, 11, 8),
    schedule=None,  
    catchup=False,
    is_paused_upon_creation=False
)
def stage():

    @task
    def start():
        EmptyOperator(task_id="start")
    
    @task
    def test_s3_connection():
        try:
            s3_hook = S3Hook()
            buckets = s3_hook.get_conn().list_buckets()['Buckets']
            bucket_name = buckets[0]['Name']  
            print(f"Successfully connected to S3. Found bucket: {bucket_name}") 
        except Exception as e:
            print(f"Error connecting to S3: {e}")
            raise

    @task
    def upload():
        try:
            s3_hook = S3Hook()
            buckets = s3_hook.get_conn().list_buckets()['Buckets']
            bucket_name = buckets[0]['Name']  

            file_path = f'/opt/airflow/dags/temp/{name}_{cycle}/out/{output_name}' 

            s3_hook.load_file(
                filename=file_path, 
                key=f'{s3_dir}/{output_name}',  
                bucket_name=bucket_name, 
                replace=True
            )

        except Exception as e:
            print(f"Error uploading file to S3: {e}")
            raise

    trigger_loading = TriggerDagRunOperator(task_id='trigger_loading', trigger_dag_id='load_data', wait_for_completion=False)

    @task
    def stop():
        EmptyOperator(task_id="stop")
    
    start() >> test_s3_connection() >> upload() >> trigger_loading >> stop()

stage()