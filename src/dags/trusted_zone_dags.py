from airflow import DAG
from datetime import datetime, timedelta
from util.dag_creation import create_dag_with_chains

SCRIPTS_SUBFOLDER = "trusted_zone/"

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(minutes=1),
    "depends_on_past": False,
}


dag_cleaning = create_dag_with_chains(
    dag_id="trusted_cleaning",
    script_layers=[[f"{SCRIPTS_SUBFOLDER}generic_cleaning.py"]],
    default_args=DEFAULT_ARGS,
)
