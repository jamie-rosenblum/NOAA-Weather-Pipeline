from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.ingest_noaa import fetch_noaa_data

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='noaa_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    ingest = PythonOperator(
        task_id='ingest_noaa_data',
        python_callable=fetch_noaa_data
    )

    dbt_run = BashOperator(
        task_id='dbt_transform',
        bash_command="cd /opt/airflow/dbt_project && dbt run"
    )

    ingest >> dbt_run