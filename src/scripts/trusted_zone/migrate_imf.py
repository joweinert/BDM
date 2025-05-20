from util.delta_storage import DeltaStorageHandler
from pyspark.sql import functions as F
from trusted_util.table_inspection import log_table_structure
from trusted_util import workflow as wf
from pyspark.sql.types import DoubleType
import trusted_util.spark_ops as sp_o
from trusted_util.gx_init import IMF_CONFIG
from trusted_util.workflow import init_logging


def imf_to_tabular(df, landing_tbl_name: str):
    """Convert IMF landing table to tabular format.

    Args:
        handler (DeltaStorageHandler): The DeltaStorageHandler instance.
        landing_tbl (str): The name of the landing table.

    Returns:
        DataFrame: The transformed DataFrame in tabular format.
    """
    # picking only the 4-digit year columns
    year_cols = [c for c in df.columns if c.isdigit() and len(c) == 4]
    if not year_cols:
        raise RuntimeError(f"No year columns in {landing_tbl_name}")

    # build a MapType column: {'1980': cast(`1980` as double), …}
    year_map_col = sp_o.build_map_from_cols(year_cols, DoubleType(), return_map=True)

    # exploding the map -> one row per (year, value)
    df_long = sp_o.explode_map(df.withColumn("year_map", year_map_col), F.col("year_map"), "year", "value")

    # adding the indicator name and cast year to int
    indicator = landing_tbl_name.split("/")[0]
    df_tab = (
        df_long.withColumn("year", F.col("year").cast("int"))
        .withColumn("indicator", F.lit(indicator))
        .select("year", "indicator", "value")
        .orderBy("year")
    )

    return df_tab


DEDUP_COLS = {
    "indicators": ["year", "indicator"],
}

CUSTOM_FN = {
    "indicators": imf_to_tabular,
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
    for asset_cfg in IMF_CONFIG.values()
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
