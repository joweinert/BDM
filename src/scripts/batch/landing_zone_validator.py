import logging
import os
from util.delta_storage import DeltaStorageHandler

log_file_path = os.path.join("/opt/bitnami/spark/logs", "landing_zone_validator.log")
logging.basicConfig(
    filename=log_file_path,
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()

if __name__ == "__main__":
    storage = DeltaStorageHandler(storage_path="Landing_Zone")
    tables = sorted(storage.list_tables())
    logger.info(f"🔍 Found {len(tables)} tables in Landing Zone.\n")

    for table_name in tables:
        try:
            logger.info(f"Table: {table_name}")
            df = storage.read_table(table_name)
            schema_str = (
                df._jdf.schema().treeString()
            )  # goes to own logging file (clean without airflowinfo)
            logger.info(f"Schema for {table_name}:\n{schema_str}")
            sample_data = df.limit(5).toPandas().to_string(index=False)
            logger.info(f"Data for {table_name}:\n{sample_data}\n")
            logger.info("-" * 60)
        except Exception as e:
            logger.error(f"❌ Failed to read {table_name}: {e}")

    storage.stop_spark()
    logger.info("✅ Done verifying Landing Zone.")
