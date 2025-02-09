from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime, now

datasets = {
    'candidate_summary': 'weball',
    'candidate_master': 'cn',
    'cand_comm_linkage': 'ccl',
    'congressional_campaigns': 'webl',
    'committee_master': 'cm',
    'pac_summary': 'webk',
    # 'individual_contributions': 'indiv',
    'committee_contributions': 'pas2',
    'committee_transactions': 'oth',
    'operating_expenditures': 'oppexp'
}

cycles = ['2024']

today = now().at(0, 0, 0) 
today_date = today.date() 
run_date = 'today' # in lieu of today_date for now
extension = '.parquet'
temp_dir = '/opt/airflow/dags/temp/'

@dag(
    dag_id='orchestrate',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_tasks=2
)
def orchestrate():
    
    @task
    def start():
        EmptyOperator(task_id="start")

    triggers = []
    for cycle in cycles:
        for name, fec_code in datasets.items():
            triggers.append(TriggerDagRunOperator(
                task_id=f"trigger_{name}",
                trigger_dag_id="fetch",
                conf={'name': name, 'fec_code': fec_code,  'cycle': cycle, 'run_date': run_date, 
                      'extension': extension, 'temp_dir': temp_dir}, 
                wait_for_completion=True
            ))

    @task
    def stop():
        EmptyOperator(task_id="stop")

    start() >> triggers >> stop()

orchestrate()