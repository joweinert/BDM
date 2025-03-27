from airflow import DAG
from datetime import datetime, timedelta
from util.spark_manager import SparkManager

SCRIPT_PATH = "s3a://pipelines/"
DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(minutes=1),
    "depends_on_past": False,
}


def create_dag_with_chains(
    dag_id, script_layers, schedule=None, default_args=DEFAULT_ARGS
):
    spark_manager = SparkManager()

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        catchup=False,
    ) as dag:
        previous_tasks = []

        for layer in script_layers:
            current_tasks = []
            for script_name in layer:
                task = spark_manager.submit_spark_job(
                    dag=dag,
                    task_id=script_name.replace(".py", ""),
                    application_path=f"{SCRIPT_PATH}{script_name}",
                )
                current_tasks.append(task)

                for prev_task in previous_tasks:
                    prev_task >> task

            previous_tasks = current_tasks

        return dag


dag_api_ingestion_pipeline = create_dag_with_chains(
    dag_id="api_ingestion_pipeline",
    script_layers=[
        ["ecb_api.py"],  # Step 1
        ["imf_api.py", "news_api.py"],  # Step 2 - parallel
        ["financial_report_api.py"],  # Step 3 --> chaineddd!
    ],
    schedule="@daily",
)


dag_ops_db_ingestion = create_dag_with_chains(
    dag_id="ops_db_ingestion",
    script_layers=[["ingest_ops_db.py"]],
)
