from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

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
    
        output_name = f'{run_date}_{name}_{cycle}{extension}'

        return {
            'name': name,
            'fec_code': fec_code,
            'cycle': cycle,
            'output_name': output_name
        }

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
    def upload(paths):

        name = paths['name']
        cycle = paths['cycle']
        output_name = paths['output_name']

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

    #trigger_loading = TriggerDagRunOperator(task_id='trigger_loading', trigger_dag_id='load_data', wait_for_completion=False)

    @task
    def stop():
        EmptyOperator(task_id="stop")
    
    config = process_config()
    paths = initialize_paths(config)
    
    start() >> test_s3_connection() >> upload(paths) >> stop()

stage()