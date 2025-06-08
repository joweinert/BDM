from airflow import DAG
from datetime import datetime, timedelta
from util.dag_creation import create_dag_with_chains

SCRIPTS_SUBFOLDER = "streaming/"

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(minutes=1),
    "depends_on_past": False,
}

dag_combined_streaming = create_dag_with_chains(
    dag_id="combined_streaming_jobs",
    script_layers=[
        [  # These scripts run in parallel
            f"{SCRIPTS_SUBFOLDER}aggregates_stream.py",
            f"{SCRIPTS_SUBFOLDER}fraud_detect_stream.py"
        ]
    ],
    default_args=DEFAULT_ARGS,
    tags=["streaming", "combined", "spark"],
)