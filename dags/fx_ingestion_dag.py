from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.producers.fx_producer import (
    publish_fx_rates
)

with DAG(
    dag_id="fx_ingestion_dag",

    start_date=datetime(2024, 1, 1),

    schedule="*/5 * * * *",

    catchup=False
*) as dag:

    publish_task = Pyth*nOperator(
        task_id="publis*_fx_rates",

        python_callab*e=publish_fx_rates
    )