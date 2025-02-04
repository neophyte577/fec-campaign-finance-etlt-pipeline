from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param
from pendulum import datetime

datasets = {
    "committee_contributions": "pas2",
    "operating_expenditures": "oppexp",
}

@dag(
    dag_id='orchestrate',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False
)
def orchestrate():
    
    @task
    def start():
        EmptyOperator(task_id="start")

    triggers = []
    for dataset_name, fec_code in datasets.items():
        triggers.append(TriggerDagRunOperator(
            task_id=f"trigger_{dataset_name}",
            trigger_dag_id="fetch",
            conf={"dataset_name": dataset_name, "fec_code": fec_code}, 
            wait_for_completion=True
        ))

    @task
    def stop():
        EmptyOperator(task_id="stop")

    start() >> triggers >> stop()

orchestrate()