from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="Kafka → Postgres ecommerce pipeline",
    schedule_interval="*/1 * * * *",  # every 1 minute
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    start_consumer = BashOperator(
        task_id="start_consumer",
        bash_command="echo 'Consumer runs as a service separately'"
    )

    validate_db = BashOperator(
        task_id="validate_db",
        bash_command="echo 'DB is reachable (replace with real check later)'"
    )

    start_consumer >> validate_db