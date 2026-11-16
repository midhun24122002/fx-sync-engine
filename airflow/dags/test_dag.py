from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def hello():
    print("FX Sync Engine DAG Running")


with DAG(
    dag_id="test_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="hello_task",
        python_callable=hello
    )