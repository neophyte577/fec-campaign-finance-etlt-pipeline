from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from pendulum import datetime, now

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

    keys = ['name', 'fec_code', 'cycle', 'run_date', 'extension', 'temp_dir']

    @task(outlets=['config'])
    def process_config(**context):
        dag_run = context['dag_run'].conf
        config = {key: dag_run.get(key) for key in keys}  
        return config

    @task(outlets=['paths'])
    def initialize_paths(config):
        name = config['name']
        fec_code = config['fec_code']
        cycle = config['cycle']
        run_date = config['run_date']
        extension = config['extension']

        current_year = now().year
        current_cycle = current_year if current_year % 2 == 0 else current_year + 1

        if int(cycle) < current_cycle:
            output_name = f'{name}_{cycle}{extension}'
        else:
            output_name = f'{run_date}_{name}_{cycle}{extension}'

        paths = {
            'name': name,
            'fec_code': fec_code,
            'cycle': cycle,
            'output_name': output_name
        }

        return paths

    @task
    def start():
        EmptyOperator(task_id="start")
    
    @task
    def test_s3_connection():
        try:
            bucket_name = 'fec-data-staging-bucket'
            s3_hook = S3Hook()

            if s3_hook.check_for_bucket(bucket_name):
                print(f"Successfully connected to S3. Found bucket: {bucket_name}")
            else:
                raise ValueError(f"Bucket {bucket_name} not found or not accessible.")
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
            bucket_name = 'fec-data-staging-bucket'

            file_path = f'/opt/airflow/dags/temp/{name}_{cycle}/out/{output_name}' 

            s3_hook.load_file(
                filename=file_path, 
                key=f'{s3_dir}/{cycle}/{output_name}',  
                bucket_name=bucket_name, 
                replace=True
            )

        except Exception as e:
            print(f"Error uploading file to S3: {e}")
            raise

    trigger_loading_task = TriggerDagRunOperator(
        task_id='trigger_loading',
        trigger_dag_id='load_data', 
        conf = {key: f'{{{{ ti.xcom_pull(task_ids="process_config")["{key}"] }}}}' for key in keys},
        wait_for_completion=False  
    )

    @task
    def stop():
        EmptyOperator(task_id="stop")

    config = process_config()
    paths = initialize_paths(config)

    start() >> test_s3_connection() >> upload(paths) >> trigger_loading_task >> stop()

stage()