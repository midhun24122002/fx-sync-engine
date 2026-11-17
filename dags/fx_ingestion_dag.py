from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def publish_fx_task():

    from src.producers.fx_producer import publish_fx_rates

    publish_fx_rates()


with DAG(
    dag_id="fx_ingestion_dag",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False
) as dag:

    publish_task = PythonOperator(
        task_id="publish_fx_rates",
        python_callable=publish_fx_task
    )