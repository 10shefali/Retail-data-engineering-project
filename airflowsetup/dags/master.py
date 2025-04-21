from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime,timedelta
from airflow.utils.state import State
from airflow.models import DagRun

## to match the latest timestamp run of the first job 
def _get_latest_execution_date(_, **kwargs):
    runs = DagRun.find(dag_id='retail_data_cleaning_dag', state=State.SUCCESS)
    if runs:
        latest = sorted(runs, key=lambda r: r.execution_date)[-1].execution_date
        return [latest]  # ← return a list!
    return [] 

default_args = {
    'owner': 'shefali',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='retail_pipeline_master_dag',
    default_args=default_args,
    description='Master DAG to orchestrate data cleaning and modeling',
    schedule_interval=None,
    start_date=datetime(2025, 4, 19),
    catchup=False,
    tags=['retail_project'],
) as dag:
    trigger_cleaning_dag = TriggerDagRunOperator(
        task_id='trigger_cleaning_dag',
        trigger_dag_id='retail_data_cleaning_dag'
    )

    wait_clean = ExternalTaskSensor(
        task_id='wait_clean',
        external_dag_id='retail_data_cleaning_dag',
        external_task_id = 'run_spark_cleaning',
        mode='poke',
        timeout=600,
        poke_interval = 30,
        execution_date_fn=_get_latest_execution_date,
        failed_states=['failed','skipped'],
        allowed_states =['success'],
    )

    trigger_modelling_dag = TriggerDagRunOperator(
        task_id= 'trigger_modelling_dag',
        trigger_dag_id='generate_model_tables_dag'
    )

    trigger_cleaning_dag>>wait_clean>>trigger_modelling_dag