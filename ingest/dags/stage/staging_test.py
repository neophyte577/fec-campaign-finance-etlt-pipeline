from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from pendulum import datetime, today

file_name = '2025-01-01_committee_contributions_2024.csv'

s3_dir = 'campaign-finance'

def upload_file_to_s3():

    try:
        s3_hook = S3Hook()
        buckets = s3_hook.get_conn().list_buckets()['Buckets']
        bucket_name = buckets[0]['Name']  

        file_path = f'/opt/airflow/dags/exploratory/{file_name}' 

        s3_hook.load_file(
            filename=file_path, 
            key=f'{s3_dir}/{file_name}',  
            bucket_name=bucket_name, 
            replace=True
        )

    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        raise

def test_s3_connection():
    try:
        s3_hook = S3Hook()
        buckets = s3_hook.get_conn().list_buckets()['Buckets']
        bucket_name = buckets[0]['Name']  
        print(f"Successfully connected to S3. Found bucket: {bucket_name}") 
    except Exception as e:
        print(f"Error connecting to S3: {e}")
        raise

with DAG(
    dag_id="s3_connection_test",
    start_date=datetime(2023, 11, 8),
    schedule=None,  
    catchup=False,
) as dag:

    test_connection = PythonOperator(
        task_id="test_s3",
        python_callable=test_s3_connection,
        provide_context=True,
    )

    upload_task = PythonOperator(
        task_id="upload_file",
        python_callable=upload_file_to_s3,
        provide_context=True,
    )

    test_connection >> upload_task