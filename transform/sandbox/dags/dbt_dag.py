from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from datetime import datetime

PROJECT_DIR = '/opt/airflow/'
PROFILES_DIR = '/opt/airflow/dbt'

# Define default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

# Initialize the DAG
with DAG('dbt_dag',
         default_args=default_args,
         schedule_interval='@daily',
         start_date=datetime(2025,1,1),
         catchup=False) as dag:
    
    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id='dbt_tasks') as dbt_tasks:
    
        stg_tpch_line_items = BashOperator(
            task_id='stg_tpch_line_items',
            bash_command='cd /opt/airflow/dbt && dbt run --models stg_tpch_line_items',
        )

        stg_tpch_orders = BashOperator(
            task_id='stg_tpch_orders',
            bash_command='cd /opt/airflow/dbt && dbt run --models stg_tpch_orders',
        )

        int_order_items_summary = BashOperator(
            task_id='int_order_items_summary',
            bash_command='cd /opt/airflow/dbt && dbt run --models int_order_items_summary',
        )

        int_order_items = BashOperator(
            task_id='int_order_items',
            bash_command='cd /opt/airflow/dbt && dbt run --models int_order_items',
        )

        fct_orders = BashOperator(
            task_id='fct_orders',
            bash_command='cd /opt/airflow/dbt && dbt run --models fct_orders',
        )
    
    with TaskGroup(group_id='dbt_tests') as dbt_tests:

        fct_orders_date_valid = BashOperator(
            task_id='fct_orders_date_valid',
            bash_command='cd /opt/airflow/dbt && dbt run -s fct_orders_date_valid',
        )

        fct_orders_discount = BashOperator(
            task_id='fct_orders_discount',
            bash_command='cd /opt/airflow/dbt && dbt run -s fct_orders_discount',
        )

    stop = EmptyOperator(task_id="stop")

    start >> [stg_tpch_line_items, stg_tpch_orders]  >> int_order_items >> int_order_items_summary >> fct_orders >> \
    [fct_orders_date_valid, fct_orders_discount] >> stop
