import requests, random, time
from pathlib import Path
from itertools import chain
import uuid
from datetime import datetime, timezone


open_kyc_requests = []
CATEGORIES = ["invoice", "receipt", "contract", "other"]
TRANSACTION_TYPES = ["deposit", "withdrawal", "payment", "transfer"]
CURRENCIES = ["USD", "EUR", "GBP", "JPY"]


def simulate_transaction(user_id):
    transaction = {
        "event": "user_transaction",
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "type": random.choice(TRANSACTION_TYPES),
        "amount": round(random.uniform(10, 5000), 2),
        "currency": random.choice(CURRENCIES),
        "description": random.choice(
            ["Coffee", "Rent", "Salary", "Transfer", "Groceries"]
        ),
    }

    try:
        res = requests.post(
            "http://app_backend:6969/submit_transaction/", json=transaction
        )
        print("💸 Transaction Response:", res.json())
    except Exception as e:
        print(f"❌ Failed to send transaction: {e}")


def get_all_pic_from_folder(folder_path):
    return list(
        chain.from_iterable(
            Path(folder_path).glob(ext)
            for ext in ["*.jpg", "*.JPG", "*.png", "*.PNG", "*.jpeg", "*.JPEG"]
        )
    )


IDS = get_all_pic_from_folder("images/ids")
FACES = get_all_pic_from_folder("images/faces")
PDFS = list(Path("pdfs").glob("*.pdf"))


def simulate_kyc(user_id):
    doc = random.choice(IDS)
    face = random.choice(FACES)

    with open(doc, "rb") as d, open(face, "rb") as f:
        res = requests.post(
            "http://app_backend:6969/upload_kyc/",
            data={"user_id": user_id},
            files={
                "doc": ("doc.jpg", d, "image/jpeg"),
                "face": ("face.jpg", f, "image/jpeg"),
            },
        )
    print("Status:", res.status_code)
    print("Raw response:", res.text)

    try:
        response = res.json()
        open_kyc_requests.append(
            {
                "request_id": response["event"]["request_id"],
                "created_at": response["event"]["upload_time"],
            }
        )
        print(response)
    except Exception as e:
        print(f"❌ Failed to parse JSON: {e}")


def simulate_pdf_upload(user_id):
    pdf = random.choice(PDFS)

    with open(pdf, "rb") as f:
        res = requests.post(
            "http://app_backend:6969/upload_pdf/",
            data={"user_id": user_id, "category": random.choice(CATEGORIES)},
            files={"file": ("document.pdf", f, "application/pdf")},
        )
    try:
        print("📄 PDF Upload Response:", res.json())
    except Exception as e:
        print(f"❌ Failed to parse JSON: {e}")


if __name__ == "__main__":
    simulate = {
        1: simulate_kyc,
        2: simulate_pdf_upload,
        3: simulate_transaction,
    }

    while True:
        user_id = f"user_{random.randint(1, 10)}"
        simulate[random.choice(list(simulate.keys()))](user_id)
        time.sleep(random.randint(30, 60))
