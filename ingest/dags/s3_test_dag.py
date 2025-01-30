from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime

def upload_file_to_s3(**kwargs):

    try:
        s3_hook = S3Hook()
        buckets = s3_hook.get_conn().list_buckets()['Buckets']
        bucket_name = buckets[0]['Name']  

        file_path = '/opt/airflow/dags/test_file.txt' 

        s3_hook.load_file(
            filename=file_path, 
            key='test_dir/uploaded_file.txt',  
            bucket_name=bucket_name, 
            replace=True
        )

        print(f"Successfully uploaded file to s3://test_dir/{bucket_name}/uploaded_file.txt")

    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        raise

with DAG(
    dag_id="s3_connection_test",
    start_date=datetime(2023, 11, 8),  
    schedule=None,  
    catchup=False,
) as dag:
    def test_s3_connection(**kwargs):
        try:
            s3_hook = S3Hook()
            buckets = s3_hook.get_conn().list_buckets()['Buckets']
            bucket_name = buckets[0]['Name']  
            print(f"Successfully connected to S3. Found bucket: {bucket_name}") 
        except Exception as e:
            print(f"Error connecting to S3: {e}")
            raise

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