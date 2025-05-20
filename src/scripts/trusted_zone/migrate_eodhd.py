"""
Transform EODHD landing snapshots (financial news) → tabular trusted table.

Input  landing: Landing_Zone/eodhd/financial_news/<timestamp>
Output trusted: Trusted_Zone/eodhd/financial_news   (one row per article)
"""

from pyspark.sql import functions as F
from util.delta_storage import DeltaStorageHandler
from trusted_util.table_inspection import log_table_structure
from trusted_util import workflow as wf
from trusted_util.gx_init import EODHD_CONFIG
from trusted_util.workflow import init_logging


def eodhd_to_tabular(df, _landing_tbl):
    """
    Flatten article‑level JSON.

    - parses ISO datetime -> timestamp
    - flattens sentiment struct into 4 numeric columns
    - leaves symbols / tags arrays intact (theyre useful as is)
    """
    return (
        df.withColumn("ts", F.to_timestamp("date"))  # keep original string too
        .select(
            "ts",
            "date",
            "title",
            "link",
            "content",
            F.col("sentiment.neg").alias("sent_neg"),
            F.col("sentiment.neu").alias("sent_neu"),
            F.col("sentiment.pos").alias("sent_pos"),
            F.col("sentiment.polarity").alias("sent_polarity"),
            "symbols",
            "tags",
        )
        .orderBy(F.col("ts").desc_nulls_last())
    )


DEDUP_COLS = {
    "financial_news": ["ts", "title", "link"],
}

CUSTOM_FN = {
    "financial_news": eodhd_to_tabular,
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
    }
    for asset_cfg in EODHD_CONFIG.values()
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
