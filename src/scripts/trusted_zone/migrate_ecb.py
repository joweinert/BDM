from util.delta_storage import DeltaStorageHandler
from trusted_util.table_inspection import log_table_structure
from trusted_util import spark_ops as s_ops
from trusted_util import workflow as wf
from trusted_util.gx_init import ECB_CONFIG
from trusted_util.workflow import init_logging


def ecb_to_tabular(df, landing_tbl: str):
    from pyspark.sql import functions as F

    # A) Explode top‐level array
    df1 = df.withColumn("ds", F.explode_outer("dataSets"))

    # B) Explode dynamic series struct
    series_map = s_ops.build_map_from_struct(df1, "ds.series")
    df2 = s_ops.explode_map(df1, F.create_map(*series_map), "series_key", "series_val")

    # C) Explode observations struct
    obs_map = s_ops.build_map_from_struct(df2, "series_val.observations")
    df3 = (
        s_ops.explode_map(
            df2.withColumn("_obs_map", F.create_map(*obs_map)), F.col("_obs_map"), "obs_index", "obs_array"
        )
        .withColumn("rate", F.col("obs_array")[0])
        .drop("obs_array")
    )

    # D) Explode the timestamp values
    times_df = df.select(
        F.posexplode(F.col("structure.dimensions.observation")[0].values).alias("obs_index", "dim")
    ).select("obs_index", F.col("dim.id").alias("time"))

    # E) Join & extract currency
    df4 = df3.join(times_df, on="obs_index")
    currency_codes = F.expr("transform(structure.dimensions.series[1].values, x->x.id)")
    df5 = df4.withColumn("currency_idx", F.split("series_key", ":")[1].cast("int")).withColumn(
        "currency", F.element_at(currency_codes, F.col("currency_idx") + 1)
    )

    # F) Final clean‐up
    return df5.withColumn("time", F.col("time").cast("timestamp")).orderBy("time").select("time", "currency", "rate")


DEDUP_COLS = {
    "exchange_rates": ["time", "currency"],
}

CUSTOM_FN = {
    "exchange_rates": ecb_to_tabular,
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
    for asset_cfg in ECB_CONFIG.values()
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
