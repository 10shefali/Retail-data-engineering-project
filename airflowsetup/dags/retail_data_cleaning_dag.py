from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

default_args = {
    'owner': 'shefali',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='retail_data_cleaning_dag',
    default_args=default_args,
    description='Run Pyspark cleaning script daily',
    schedule_interval='@daily',
    catchup=False
) as dag:

    run_spark_cleaning = DockerOperator(
        task_id='run_spark_cleaning',
        image='bitnami/spark:latest',
        api_version='auto',
        auto_remove=True,
        command="/opt/bitnami/spark/bin/spark-submit /opt/sparkjobs/data_cleaning.py",
        docker_url='unix://var/run/docker.sock',
        mounts=[
            Mount(
                source="/mnt/c/Users/Shefali/Documents/Retail_project/airflowsetup/dags/sparkjobs",
                target="/opt/sparkjobs",
                type="bind"
        )
    ]
    )

    run_spark_cleaning
