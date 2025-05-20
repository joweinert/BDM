"""
Transform Finnhub landing snapshots (company financial reports) → tabular trusted table.

Input  landing: Landing_Zone/finnhub/<SYMBOL>_financial_report/<timestamp>
Output trusted: Trusted_Zone/finnhub/financial_report   (one row per line‑item)
"""

from pyspark.sql import functions as F
from util.delta_storage import DeltaStorageHandler
from trusted_util.table_inspection import log_table_structure
from trusted_util import workflow as wf
from trusted_util.gx_init import FINNHUB_CONFIG
from trusted_util.workflow import init_logging


def _explode_statement(df, col, statement_name):
    """Helper: explode a single statement array (bs / cf / ic)"""
    return df.select(
        "symbol",
        "year",
        "quarter",
        F.explode_outer(col).alias("item"),
    ).select(
        "symbol",
        "year",
        "quarter",
        F.lit(statement_name).alias("statement"),
        F.col("item.concept").alias("concept"),
        F.col("item.label").alias("label"),
        F.col("item.unit").alias("unit"),
        F.col("item.value").alias("value"),
    )


def finnhub_to_tabular(df, _landing_tbl):
    """
    Flatten bs / cf / ic arrays into long format:

    ┌symbol─year─qtr─statement─concept───────────label─────────unit─value┐
    │AAPL  │2025│1  │bs        │us-gaap_Assets│Total assets  │usd │…   │
    """
    bs = _explode_statement(df, "report.bs", "bs")
    cf = _explode_statement(df, "report.cf", "cf")
    ic = _explode_statement(df, "report.ic", "ic")

    return bs.unionByName(cf).unionByName(ic).orderBy("symbol", "year", "quarter", "statement", "concept")


DEDUP_COLS = {
    "financial_report": ["symbol", "year", "quarter", "statement", "concept"],
}

CUSTOM_FN = {
    "financial_report": finnhub_to_tabular,
}

CONFIGS = [
    {
        # shared base paths
        "trusted_storage_path": "Trusted_Zone",
        "landing_table": f"Landing_Zone/{asset_cfg['data_source_name']}",
        # per-asset targets
        "trusted_datasource": asset_cfg["data_source_name"],
        "trusted_dataset": asset_cfg["data_asset_name"],
        "trusted_table": f"{asset_cfg['data_source_name']}/{asset_cfg['data_asset_name']}",
        "dedup_cols": DEDUP_COLS[asset_cfg["data_asset_name"]],
        "custom_fn": CUSTOM_FN[asset_cfg["data_asset_name"]],
        **asset_cfg,
    }
    for asset_cfg in FINNHUB_CONFIG.values()
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
