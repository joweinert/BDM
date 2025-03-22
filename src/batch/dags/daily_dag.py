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
) as dag1:

    # Initialize SparkManager
    spark_manager = SparkManager()

    # Use SparkManager to submit the job
    ecb_api_ingestion = spark_manager.submit_spark_job(
        dag=dag1,
        task_id="ecb_api_ingestion",
        application_path="s3a://pipelines/ecb_api.py",
    )

    ecb_api_ingestion



# Define the second DAG
with DAG(
    "imf_api_ingestion",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag2:

    # Use SparkManager to submit the second job
    imf_api_ingestion = spark_manager.submit_spark_job(
        dag=dag2,
        task_id="imf_api_ingestion",
        application_path="s3a://pipelines/imf_api.py",
    )

    imf_api_ingestion

# Define a single DAG
with DAG(
    "face_api_ingestion",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag3:

    # Task 1: EODHD API ingestion
    face_api_ingestion = spark_manager.submit_spark_job(
        dag=dag3,
        task_id="face_api_ingestion",
        application_path="s3a://pipelines/face_api.py",
    )

    face_api_ingestion