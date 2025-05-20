import logging
import os
from util.delta_storage import DeltaStorageHandler

log_file_path = os.path.join("/opt/bitnami/spark/logs", "delta_tables.log")
logging.basicConfig(
    filename=log_file_path,
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()

if __name__ == "__main__":
    storage = DeltaStorageHandler(storage_path="")
    tables = sorted(storage.list_tables())
    if not tables:
        logger.info("❌ No tables found.")
        exit(0)
    logger.info(f"🔍 Found {len(tables)} tables.\n")

    for table_name in tables:
        try:
            logger.info(f"Table: {table_name}")
            df = storage.read_table(table_name)
            schema_str = df._jdf.schema().treeString()
            logger.info(f"Schema for {table_name}:\n{schema_str}")
            try:
                logger.info("=== Sample Data (5 rows) ===\n")
                for row in df.limit(5).collect():
                    logger.info(str(row))
            except Exception as e:
                logger.warning(f"⚠️ Error collecting sample: {e}\n")
            logger.info("-" * 60)
        except Exception as e:
            logger.error(f"❌ Failed to read {table_name}: {e}")

    storage.stop_spark()
    logger.info("✅ Done.")
