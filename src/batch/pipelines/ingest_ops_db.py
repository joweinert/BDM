import os, uuid
import psycopg2
import boto3
from PIL import Image
from io import BytesIO
from util.delta_storage import DeltaStorageHandler

# Load env vars
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


def extract_image_metadata(s3_path):
    # Ex: s3a://bucket/images/filename.jpg
    path = s3_path.replace("s3a://", "")
    bucket, key = path.split("/", 1)

    response = S3_CLIENT.get_object(Bucket=bucket, Key=key)
    img_bytes = response["Body"].read()
    img = Image.open(BytesIO(img_bytes))
    width, height = img.size
    fmt = img.format
    size_bytes = len(img_bytes)

    return {
        "width": width,
        "height": height,
        "format": fmt,
        "size_bytes": size_bytes,
    }


def mark_as_exported(table_name, id_column, ids):
    if not ids:
        return
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    cur.execute(
        f"UPDATE {table_name} SET status = 'exported' WHERE {id_column} = ANY(%s::uuid[])",
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


def enrich_kyc(jobs):
    all_entries = []
    for job in jobs:
        try:
            request_id, user_id, doc_path, face_path, upload_time, status = job
            doc_meta = extract_image_metadata(doc_path)
            face_meta = extract_image_metadata(face_path)

            for short_desc, path, tags, meta in [
                ("ID Document", doc_path, ["kyc", "id"], doc_meta),
                ("Face Photo", face_path, ["kyc", "face"], face_meta),
            ]:
                all_entries.append(
                    {
                        "image_id": str(uuid.uuid4()),
                        "request_id": request_id,
                        "user_id": user_id,
                        "short_desc": short_desc,
                        "s3_path": path,
                        "upload_time": upload_time.isoformat(),
                        "status": status,
                        "tags": tags,
                        **meta,
                    }
                )

        except Exception as e:
            print(f"❌ Failed to process {job[0]}: {e}")
    return all_entries


def extract_pdf_metadata(s3_path):
    path = s3_path.replace("s3a://", "")
    bucket, key = path.split("/", 1)

    response = S3_CLIENT.get_object(Bucket=bucket, Key=key)
    pdf_bytes = response["Body"].read()
    size_bytes = len(pdf_bytes)

    return {"format": "PDF", "size_bytes": size_bytes, "tags": ["pdf", "user_upload"]}


def enrich_pdfs(jobs):
    all_entries = []
    for job in jobs:
        try:
            request_id, user_id, category, pdf_path, upload_time = job
            meta = extract_pdf_metadata(pdf_path)

            all_entries.append(
                {
                    "pdf_id": str(uuid.uuid4()),
                    "request_id": request_id,
                    "user_id": user_id,
                    "category": category,
                    "s3_path": pdf_path,
                    "upload_time": upload_time.isoformat(),
                    **meta,
                }
            )

        except Exception as e:
            print(f"❌ Failed to process PDF {job[0]}: {e}")
    return all_entries


def enrich_transactions(rows):
    return [
        {
            "transaction_id": tx_id,
            "user_id": user_id,
            "timestamp": ts.isoformat(),
            "type": tx_type,
            "amount": float(amount),
            "currency": currency,
            "description": desc,
            "tags": ["transaction", tx_type],
        }
        for tx_id, user_id, ts, tx_type, amount, currency, desc in rows
    ]


JOB_CONFIG = {
    "kyc_jobs": {
        "query": """
            SELECT request_id, user_id, doc_s3_path, face_s3_path, upload_time, status
            FROM kyc_jobs WHERE status != 'exported'
        """,
        "enricher": enrich_kyc,
        "id_column": "request_id",
        "delta_table": "kyc_metadata",
    },
    "pdf_uploads": {
        "query": """
            SELECT request_id, user_id, category, pdf_s3_path, upload_time
            FROM pdf_uploads WHERE status IS DISTINCT FROM 'exported'
        """,
        "enricher": enrich_pdfs,
        "id_column": "request_id",
        "delta_table": "pdf_metadata",
    },
    "user_transactions": {
        "query": """
            SELECT transaction_id, user_id, timestamp, type, amount, currency, description
            FROM user_transactions WHERE status != 'exported'
        """,
        "enricher": enrich_transactions,
        "id_column": "transaction_id",
        "delta_table": "user_transactions",
    },
}


if __name__ == "__main__":
    for table_name, config in JOB_CONFIG.items():
        print(f"🔎 Checking for pending jobs in: {table_name}")

        rows = fetch(table_name=table_name, query=config["query"])
        print(f"📦 Found {len(rows)} jobs in {table_name}")

        enriched = config["enricher"](rows)

        if enriched:
            storage.write_api_json(enriched, table_name=config["delta_table"])
            print(
                f"✅ Wrote {len(enriched)} records to Delta table '{config['delta_table']}'"
            )
            ids = [row[0] for row in rows]
            mark_as_exported(
                table_name=table_name, id_column=config["id_column"], ids=ids
            )
        else:
            print(f"⚠️ No records to write for {table_name}")

    storage.stop_spark()
    print("✅ All jobs exported to Delta Lake")
