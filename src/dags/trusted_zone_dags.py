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
    dag_id="trusted_migration",
    script_layers=[
        [f"{SCRIPTS_SUBFOLDER}trusted_util/gx_init.py"],
        [f"{SCRIPTS_SUBFOLDER}migrate_ops_db.py"],
        [f"{SCRIPTS_SUBFOLDER}migrate_eodhd.py", f"{SCRIPTS_SUBFOLDER}migrate_finnhub.py"],
        [f"{SCRIPTS_SUBFOLDER}migrate_ecb.py", f"{SCRIPTS_SUBFOLDER}migrate_imf.py"],
    ],
    default_args=DEFAULT_ARGS,
    tags=["trusted"],
)
