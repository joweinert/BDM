from airflow import DAG
from datetime import datetime, timedelta
from util.dag_creation import create_dag_with_chains

SCRIPTS_SUBFOLDER = "batch/"

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(minutes=1),
    "depends_on_past": False,
}


dag_all_jobs_in_sequence = create_dag_with_chains(
    dag_id="recommended_test_all_jobs_in_sequence",
    script_layers=[
        [f"{SCRIPTS_SUBFOLDER}ecb_api.py"],
        [f"{SCRIPTS_SUBFOLDER}imf_api.py"],
        [f"{SCRIPTS_SUBFOLDER}news_api.py"],
        [f"{SCRIPTS_SUBFOLDER}financial_report_api.py"],
        [f"{SCRIPTS_SUBFOLDER}ingest_ops_db.py"],
        [f"delta_logging.py"],
    ],
    default_args=DEFAULT_ARGS,
    tags=["batch", "test"],
)


dag_api_ingestion_pipeline = create_dag_with_chains(
    dag_id="basically_just_bragging_with_random_dependencies_and_parallelism",
    script_layers=[
        [f"{SCRIPTS_SUBFOLDER}ecb_api.py"],  # Step 1
        [f"{SCRIPTS_SUBFOLDER}imf_api.py", f"{SCRIPTS_SUBFOLDER}news_api.py"],  # Step 2 - parallel
        [f"{SCRIPTS_SUBFOLDER}financial_report_api.py"],  # Step 3 --> chaineddd!
    ],
    schedule="@daily",
    default_args=DEFAULT_ARGS,
    tags=["batch"],
)


dag_ecb_api = create_dag_with_chains(
    dag_id="ecb_api",
    script_layers=[[f"{SCRIPTS_SUBFOLDER}ecb_api.py"]],
    tags=["batch", "ecb"],
)


dag_news_api = create_dag_with_chains(
    dag_id="news_api",
    script_layers=[[f"{SCRIPTS_SUBFOLDER}news_api.py"]],
    default_args=DEFAULT_ARGS,
    tags=["batch", "news"],
)


dag_financial_report_api = create_dag_with_chains(
    dag_id="financial_report_api",
    script_layers=[[f"{SCRIPTS_SUBFOLDER}financial_report_api.py"]],
    schedule="0 0 1 3,9 *",  # 00.00 first of March and September
    default_args=DEFAULT_ARGS,
    tags=["batch", "financial_report"],
)


dag_ops_db_ingestion = create_dag_with_chains(
    dag_id="ops_db_ingestion",
    script_layers=[[f"{SCRIPTS_SUBFOLDER}ingest_ops_db.py"]],
    default_args=DEFAULT_ARGS,
    tags=["batch", "ops_db"],
)


dag_imf_api = create_dag_with_chains(
    dag_id="imf_api",
    script_layers=[[f"{SCRIPTS_SUBFOLDER}imf_api.py"]],
    default_args=DEFAULT_ARGS,
    tags=["batch", "imf"],
)


dag_delta_logging = create_dag_with_chains(
    dag_id="delta_logging",
    script_layers=[[f"delta_logging.py"]],
    default_args=DEFAULT_ARGS,
    tags=["test"],
)
