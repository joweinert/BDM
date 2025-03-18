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

# Define a single DAG
with DAG(
    "api_ingestion_combined",
    default_args=default_args,
    schedule_interval="@monthly",
    catchup=False,
) as dag:

    # Initialize a single SparkManager instance
    spark_manager = SparkManager()

    # Task 1: EODHD API ingestion
    eodhd_api_ingestion = spark_manager.submit_spark_job(
        dag=dag,
        task_id="eodhd_api_ingestion",
        application_path="s3a://pipelines/news_api.py",
    )

    # Task 2: Finnhub API ingestion
    finnhub_api_ingestion = spark_manager.submit_spark_job(
        dag=dag,
        task_id="finnhub_api_ingestion",
        application_path="s3a://pipelines/financial_report_api.py",
    )

    # Set dependencies
    eodhd_api_ingestion >> finnhub_api_ingestion