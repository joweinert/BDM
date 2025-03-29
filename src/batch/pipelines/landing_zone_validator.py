from util.delta_storage import DeltaStorageHandler

if __name__ == "__main__":
    storage = DeltaStorageHandler(storage_path="Landing_Zone")
    tables = storage.list_tables()

    print(f"🔍 Found {len(tables)} tables in Landing Zone.\n")

    for table_name in tables:
        try:
            print(f"📦 Table: {table_name}")
            df = storage.read_table(table_name)
            df.printSchema()
            df.show(5, truncate=False)
            print("\n" + "-" * 60 + "\n")
        except Exception as e:
            print(f"❌ Failed to read {table_name}: {e}")

    storage.stop_spark()
    print("✅ Done verifying Landing Zone.")
