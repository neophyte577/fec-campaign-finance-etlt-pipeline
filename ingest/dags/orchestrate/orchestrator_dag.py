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
    'individual_contributions': 'indiv',
    'committee_contributions': 'pas2',
    'committee_transactions': 'oth',
    'operating_expenditures': 'oppexp'
}

cycles = ['2026']

# cycles = ['2026', '2024', '2022', '2020', '2018', '2016', '2014','2012', '2010', '2008', '2006', '2004', '2002', '2000', 
#           '1998', '1996', '1994', '1992', '1990', '1988', '1986', '1984', '1982', '1980'] 

today_date = now().at(0, 0, 0).date() 
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
                task_id=f"trigger_{name}_{cycle}",
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