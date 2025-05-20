from util.delta_storage import DeltaStorageHandler
from trusted_util.table_inspection import log_table_structure
from trusted_util import workflow as wf

from trusted_util.gx_init import OPS_DB_CONFIG
from trusted_util.workflow import init_logging

DEDUP_COLS = {
    "user_transactions": ["transaction_id"],
    "user_profiles": ["user_id"],
    "pdf_metadata": ["request_id"],
    "kyc_metadata": ["request_id"],
}

CUSTOM_FN = {
    "user_transactions": None,
    "user_profiles": None,
    "pdf_metadata": None,
    "kyc_metadata": None,
}

FILE_INTEGRITY_CHECKS = {
    "pdf_metadata": {
        "enabled": True,
        "columns": ["pdf_s3_path"],
        "bytes_to_check": 512,
    },
    "kyc_metadata": {
        "enabled": True,
        "columns": ["face_s3_path", "doc_s3_path"],
        "bytes_to_check": 512,
    },
}

CONFIGS = [
    {
        # shared base paths
        "trusted_storage_path": "Trusted_Zone",
        "landing_table": f"Landing_Zone/{asset_cfg['data_source_name']}/{asset_cfg['data_asset_name']}",
        # per-asset targets
        "trusted_datasource": asset_cfg["data_source_name"],
        "trusted_dataset": asset_cfg["data_asset_name"],
        "trusted_table": f"{asset_cfg['data_source_name']}/{asset_cfg['data_asset_name']}",
        "dedup_cols": DEDUP_COLS[asset_cfg["data_asset_name"]],
        "custom_fn": CUSTOM_FN[asset_cfg["data_asset_name"]],
        **asset_cfg,
        "file_integrity": FILE_INTEGRITY_CHECKS.get(asset_cfg["data_asset_name"], {"enabled": False}),
    }
    for asset_cfg in OPS_DB_CONFIG.values()
]


if __name__ == "__main__":
    init_logging()
    for cfg in CONFIGS:
        print(f"Migrating {cfg['data_asset_name']}…")
        handler = DeltaStorageHandler(storage_path=cfg["landing_table"])
        try:
            df = wf.migrate(handler, cfg)
            log_table_structure(df)
        finally:
            handler.stop_spark()
