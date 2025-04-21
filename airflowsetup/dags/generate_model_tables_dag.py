from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'shefali',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='generate_model_tables_dag',
    default_args=default_args,
    description='Create fact and dimension table',
    schedule_interval=None,
    catchup=False,
    tags=['retail_project'],
    start_date=datetime(2025, 4, 1),  # Add this to match the sensor's requirement
) as dag:

    wait_for_cleaning = ExternalTaskSensor(
        task_id='wait_for_cleaning',
        external_dag_id='retail_data_cleaning_dag',
        external_task_id='run_spark_cleaning',
        mode='poke',
        timeout=600,  # 10 minutes
        poke_interval=30,  # Check every 30 seconds
        allowed_states=['success'],
        failed_states=['failed', 'skipped']
    )

    modelling_table = DockerOperator(
        task_id='modelling_table',
        image='bitnami/spark:latest',
        api_version='auto',
        auto_remove=True,
        network_mode="airflow",
        command="/opt/bitnami/spark/bin/spark-submit /opt/sparkjobs/generate_model_tables.py",
        docker_url='unix://var/run/docker.sock',
        mounts=[
            Mount(
                source="/mnt/c/Users/Shefali/Documents/Retail_project/airflowsetup/dags/sparkjobs",
                target="/opt/sparkjobs",
                type="bind"
            )
        ]
    )

    wait_for_cleaning >> modelling_table
