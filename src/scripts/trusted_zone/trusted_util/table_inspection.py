from util.delta_storage import DeltaStorageHandler
import datetime, sys
from contextlib import nullcontext
import os

TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_PATH = f"/opt/bitnami/spark/logs/delta_inspect"
LOG_FILE = f"{LOG_PATH}/{TIMESTAMP}_table_inspection.log"

def log_table_structure(df, table_name="", unique_val=None, writer=None):
    """
    Log the structure of the DataFrame to a file.
    """
    if not os.path.exists(LOG_PATH):
        os.mkdir(LOG_PATH)
    cm = open(LOG_FILE, "w") if writer is None else nullcontext(writer)
    
    if not df:
        return

    with cm as f:
        f.write(f"=== Table: {table_name} ===\n\n")
        # Schema
        f.write("=== Schema ===\n")
        f.write(df._jdf.schema().treeString() + "\n\n")

        # Count
        try:
            row_count = df.count()
            f.write("=== Row Count ===\n")
            f.write(str(row_count) + "\n\n")
        except Exception as e:
            f.write(f"⚠️ Error counting rows: {e}\n\n")

        # Unique Values
        if unique_val:
            f.write("=== Unique Values ===\n")
            for col in unique_val:
                try:
                    unique_vals = df.select(col).distinct()
                    f.write(f"Unique values for {col}:\n")
                    for row in unique_vals.collect():
                        f.write(str(row) + "\n")
                    unique_count = unique_vals.count()
                    f.write(f"{col}: {unique_count} unique values\n")
                except Exception as e:
                    f.write(f"⚠️ Error counting unique values for {col}: {e}\n")

        # Sample
        try:
            f.write("=== Sample Data (5 rows) ===\n")
            for row in df.limit(5).collect():
                f.write(str(row) + "\n")
        except Exception as e:
            f.write(f"⚠️ Error collecting sample: {e}\n")

    print(f"✅ Inspection written to {LOG_FILE}")
    return 0


if __name__ == "__main__":
    base_path = sys.argv[1] if len(sys.argv) > 1 else "Trusted_Zone/imf"

    handler = DeltaStorageHandler(storage_path=base_path)
    # latest = sorted(handler.list_tables())[-1]
    try:
        with open(LOG_FILE, "w") as f:
            for table in handler.list_tables():
                df = handler.read_table(table)
                log_table_structure(df, table_name=table, writer=f)
    finally:
        handler.stop_spark()
