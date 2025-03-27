from fastapi import FastAPI, UploadFile, Form
from kafka import KafkaProducer
from PIL import Image
import uuid, json, os
from datetime import datetime, timezone
from io import BytesIO
import psycopg2
import boto3

app = FastAPI()


DB_CONFIG = {
    "host": os.getenv("OPS_DB_HOST"),
    "port": os.getenv("OPS_DB_PORT"),
    "dbname": os.getenv("OPS_DB_NAME"),
    "user": os.getenv("OPS_DB_USER"),
    "password": os.getenv("OPS_DB_PASS"),
}


S3_CLIENT = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
)
MINIO_UNSTRUCTURED = os.getenv("MINIO_UNSTRUCTURED_BUCKET")

PRODUCER = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def save_image_metadata_to_postgres(
    request_id, user_id, doc_path, face_path, timestamp
):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS kyc_jobs (
            request_id UUID PRIMARY KEY,
            user_id TEXT,
            doc_s3_path TEXT,
            face_s3_path TEXT,
            upload_time TIMESTAMP,
            status TEXT DEFAULT 'pending'
        );
    """
    )
    cur.execute(
        """
        INSERT INTO kyc_jobs (request_id, user_id, doc_s3_path, face_s3_path, upload_time)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (request_id) DO NOTHING;
    """,
        (request_id, user_id, doc_path, face_path, timestamp),
    )
    conn.commit()
    cur.close()
    conn.close()


def save_pdf_metadata_to_postgres(request_id, user_id, category, pdf_path, timestamp):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pdf_uploads (
            request_id UUID PRIMARY KEY,
            user_id TEXT,
            category TEXT,
            pdf_s3_path TEXT,
            upload_time TIMESTAMP,
            status TEXT DEFAULT 'pending'
        );
        """
    )
    cur.execute(
        """
        INSERT INTO pdf_uploads (request_id, user_id, category, pdf_s3_path, upload_time)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (request_id) DO NOTHING;
        """,
        (request_id, user_id, category, pdf_path, timestamp),
    )
    conn.commit()
    cur.close()
    conn.close()


def save_transaction_to_postgres(tx):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_transactions (
            transaction_id UUID PRIMARY KEY,
            user_id TEXT,
            timestamp TIMESTAMPTZ,
            type TEXT,
            amount NUMERIC,
            currency TEXT,
            description TEXT,
            status TEXT DEFAULT 'pending'
        );
    """
    )
    cur.execute(
        """
        INSERT INTO user_transactions (transaction_id, user_id, timestamp, type, amount, currency, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO NOTHING;
    """,
        (
            tx["transaction_id"],
            tx["user_id"],
            tx["timestamp"],
            tx["type"],
            tx["amount"],
            tx["currency"],
            tx["description"],
        ),
    )
    conn.commit()
    cur.close()
    conn.close()


def upload_image_to_minio(image_obj, filename) -> str:
    with BytesIO() as buffer:
        image_obj.save(buffer, format="JPEG")
        buffer.seek(0)

        S3_CLIENT.upload_fileobj(buffer, MINIO_UNSTRUCTURED, "images/" + filename)
        return f"s3a://{MINIO_UNSTRUCTURED}/images/{filename}"


@app.post("/upload_kyc/")
async def upload_kyc(
    user_id: str = Form(...), doc: UploadFile = Form(...), face: UploadFile = Form(...)
):
    doc_image = Image.open(BytesIO(await doc.read()))
    face_image = Image.open(BytesIO(await face.read()))

    request_id = str(uuid.uuid4())
    doc_s3_path = upload_image_to_minio(doc_image, f"doc_{request_id}.jpg")
    face_s3_path = upload_image_to_minio(face_image, f"face_{request_id}.jpg")

    event = {
        "event": "kyc_request",
        "request_id": request_id,
        "user_id": user_id,
        "doc_s3_path": doc_s3_path,
        "face_s3_path": face_s3_path,
        "upload_time": datetime.now(timezone.utc).isoformat(),
        "status": "pending",
    }

    save_image_metadata_to_postgres(
        request_id=event["request_id"],
        user_id=event["user_id"],
        doc_path=event["doc_s3_path"],
        face_path=event["face_s3_path"],
        timestamp=event["upload_time"],
    )

    PRODUCER.send("fraud_detect", value=event)
    return {"status": "submitted", "event": event}


@app.post("/upload_pdf/")
async def upload_pdf(
    user_id: str = Form(...), category: str = Form(...), file: UploadFile = Form(...)
):
    request_id = str(uuid.uuid4())
    pdf_s3_path = f"s3a://{MINIO_UNSTRUCTURED}/pdfs/{request_id}.pdf"

    with BytesIO() as buffer:
        buffer.write(await file.read())
        buffer.seek(0)

        S3_CLIENT.upload_fileobj(buffer, MINIO_UNSTRUCTURED, f"pdfs/{request_id}.pdf")

    event = {
        "event": "pdf_upload",
        "request_id": request_id,
        "user_id": user_id,
        "s3_path": pdf_s3_path,
        "upload_time": datetime.now(timezone.utc).isoformat(),
    }

    save_pdf_metadata_to_postgres(
        request_id=event["request_id"],
        user_id=event["user_id"],
        category=category,
        pdf_path=event["s3_path"],
        timestamp=event["upload_time"],
    )

    PRODUCER.send("fraud_detect", value=event)
    return {"status": "submitted", "event": event}


@app.post("/submit_transaction/")
async def submit_transaction(payload: dict):
    save_transaction_to_postgres(payload)
    PRODUCER.send("fraud_detect", value=payload)
    return {"status": "received", "event": payload}


@app.get("/health")
def healthcheck():
    return {"status": "ok"}
