from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="kafka_ecommerce_pipeline",
    default_args=default_args,
    schedule="*/5 * * * *",
    catchup=False,
    description="Generate analytics report"
) as dag:

    run_report = BashOperator(
        task_id="run_report",
        bash_command="python /opt/airflow/scripts/analytics/report.py"
    )