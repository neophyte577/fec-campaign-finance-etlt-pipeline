from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from datetime import datetime

DBT_DIR = '/opt/airflow/dbt/transformations'

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG('dbt_dag',
         default_args=default_args,
         schedule_interval='@daily',
         start_date=datetime(2025,1,1),
         catchup=False) as dag:
    
    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id='staging') as staging:
    
        stg_candidates = BashOperator(
            task_id='stg_candidates',
            bash_command=f'cd {DBT_DIR} && dbt run --models stg_candidates',
        )

        stg_committees = BashOperator(
            task_id='stg_committees',
            bash_command=f'cd {DBT_DIR} && dbt run --models stg_committees',
        )

        stg_committee_contributions = BashOperator(
            task_id='stg_committee_contributions',
            bash_command=f'cd {DBT_DIR} && dbt run --models stg_committee_contributions',
        )

        stg_committee_transactions = BashOperator(
            task_id='stg_committee_transactions',
            bash_command=f'cd {DBT_DIR} && dbt run --models stg_committee_transactions',
        )

        stg_individual_contributions = BashOperator(
            task_id='stg_individual_contributions',
            bash_command=f'cd {DBT_DIR} && dbt run --models stg_individual_contributions',
        )

        stg_operating_expenditures = BashOperator(
            task_id='stg_operating_expenditures',
            bash_command=f'cd {DBT_DIR} && dbt run --models stg_operating_expenditures',
        )

    with TaskGroup(group_id='marts') as marts:
    
        fct_committee_contributions = BashOperator(
            task_id='fct_committee_contributions',
            bash_command=f'cd {DBT_DIR} && dbt run --models fct_committee_contributions',
        )

        agg_indiv_contr_by_cand = BashOperator(
            task_id='agg_indiv_contr_by_cand',
            bash_command=f'cd {DBT_DIR} && dbt run --models agg_indiv_contr_by_cand',
        )

        agg_indiv_contr_by_state = BashOperator(
            task_id='agg_indiv_contr_by_state',
            bash_command=f'cd {DBT_DIR} && dbt run --models agg_indiv_contr_by_state',
        )

        agg_oper_exp_by_cand = BashOperator(
            task_id='agg_oper_exp_by_cand',
            bash_command=f'cd {DBT_DIR} && dbt run --models agg_oper_exp_by_cand',
        )

        agg_oper_exp_by_comm = BashOperator(
            task_id='agg_oper_exp_by_comm',
            bash_command=f'cd {DBT_DIR} && dbt run --models agg_oper_exp_by_comm',
        )

        agg_oper_exp_categories = BashOperator(
            task_id='agg_oper_exp_categories',
            bash_command=f'cd {DBT_DIR} && dbt run --models agg_oper_exp_categories',
        )
    
    with TaskGroup(group_id='tests') as tests:

        fct_orders_date_valid = BashOperator(
            task_id='fct_orders_date_valid',
            bash_command=f'cd {DBT_DIR} && dbt run -s fct_orders_date_valid',
        )

        fct_orders_discount = BashOperator(
            task_id='fct_orders_discount',
            bash_command=f'cd {DBT_DIR} && dbt run -s fct_orders_discount',
        )

    stop = EmptyOperator(task_id="stop")

    start >> staging  >> marts >> tests >>  stop
