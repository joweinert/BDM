from airflow import DAG
from util.spark_manager import SparkManager


def create_dag_with_chains(dag_id, script_layers, schedule=None, default_args=None, tags=None):
    spark_manager = SparkManager()

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        tags=tags,
        catchup=False,
    ) as dag:
        previous_tasks = []

        for layer in script_layers:
            current_tasks = []
            for script_name in layer:
                task = spark_manager.submit_spark_job(
                    dag=dag,
                    task_id=script_name.replace("/", "_").replace("\\", "_").replace(".py", ""),
                    application=f"{script_name}",
                )
                current_tasks.append(task)

                for prev_task in previous_tasks:
                    prev_task >> task

            previous_tasks = current_tasks

        return dag
