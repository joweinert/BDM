import os
import psycopg2
import boto3
from util.delta_storage import DeltaStorageHandler

POSTGRES_CONFIG = {
    "host": os.getenv("OPS_DB_HOST"),
    "port": os.getenv("OPS_DB_PORT"),
    "dbname": os.getenv("OPS_DB_NAME"),
    "user": os.getenv("OPS_DB_USER"),
    "password": os.getenv("OPS_DB_PASS"),
}
S3_BUCKET = os.getenv("MINIO_DATA_BUCKET")
S3_CLIENT = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
)


storage = DeltaStorageHandler()


def table_exists(table_name, schema="public"):
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = %s
            AND    table_name   = %s
        );
    """,
        (schema, table_name),
    )
    exists = cur.fetchone()[0]
    cur.close()
    conn.close()
    return exists


def mark_as_exported(table_name, id_column, ids):
    if not ids:
        return
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    cur.execute(
        f"UPDATE {table_name} SET status = 'exported' WHERE {id_column} = ANY(%s)",
        (ids,),
    )
    conn.commit()
    cur.close()
    conn.close()


def fetch(table_name, query, schema="public"):
    if not table_exists(table_name, schema):
        return []
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


JOB_CONFIG = {
    "kyc_jobs": {
        "columns": [
            "request_id",
            "user_id",
            "doc_s3_path",
            "face_s3_path",
            "upload_time",
            "status",
        ],
        "id_column": "request_id",
        "delta_table": "kyc_metadata",
    },
    "pdf_uploads": {
        "columns": [
            "request_id",
            "user_id",
            "category",
            "pdf_s3_path",
            "upload_time",
        ],
        "id_column": "request_id",
        "delta_table": "pdf_metadata",
    },
    "user_transactions": {
        "columns": [
            "transaction_id",
            "user_id",
            "timestamp",
            "type",
            "amount",
            "currency",
            "description",
            "location",
            "device_id",
            "ip_address",
            "account_balance",
            "previous_transaction",
            "transaction_duration",
            "login_attempts",
        ],
        "id_column": "transaction_id",
        "delta_table": "user_transactions",
    },
    "users": {
        "columns": ["user_id", "age", "occupation"],
        "id_column": "user_id",
        "delta_table": "user_profiles",
    },
}


if __name__ == "__main__":
    for table, config in JOB_CONFIG.items():
        print(f"🔎 Checking for pending jobs in: {table}")

        column_list = ", ".join(config["columns"])
        query = f"SELECT {column_list} FROM {table} WHERE status != 'exported'"
        rows = fetch(table_name=table, query=query)
        if not rows:
            print(f"✅ No pending jobs found in {table}")
            continue
        print(f"📦 Found {len(rows)} jobs in {table}")

        table_name = storage.write_relational_data(
            rows=rows,
            columns=config["columns"],
            datasource="ops_db",
            dataset=config["delta_table"],
        )
        print(f"✅ Wrote {len(rows)} records to Delta table '{table_name}'")
        ids = [row[0] for row in rows]
        mark_as_exported(table_name=table, id_column=config["id_column"], ids=ids)

    storage.stop_spark()
    print("✅ All jobs exported to Delta Lake")
