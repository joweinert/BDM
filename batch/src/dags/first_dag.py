from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime.today(),
    "depends_on_past": False,
    "retries": 1,
}

dag = DAG(
    "spark_delta_ingestion",
    default_args=default_args,
    schedule_interval="@daily",  # Run daily (change as needed)
    catchup=False,
)

run_spark_script = BashOperator(
    task_id="run_spark_script",
    bash_command="docker exec delta-spark spark-submit /scripts/ecb_api.py",
    dag=dag,
)

run_spark_script
