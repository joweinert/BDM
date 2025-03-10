from airflow import DAG
from datetime import datetime, timedelta
from util.spark_manager import SparkManager

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 7),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "ecb_api_ingestion",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Initialize SparkManager
    spark_manager = SparkManager()

    # Use SparkManager to submit the job
    ecb_api_ingestion = spark_manager.submit_spark_job(
        dag=dag,
        task_id="ecb_api_ingestion",
        application_path="s3a://pipelines/ecb_api.py",
    )

    ecb_api_ingestion
