import requests, random, time, os
from pathlib import Path
from itertools import chain
import uuid
from datetime import datetime, timezone
import kagglehub
import pandas as pd

open_kyc_requests = []
BIG_DATA = os.getenv("BIGDATA_MODE", "false").lower() == "true"
CURRENCIES = ["USD", "EUR", "JPY", "GBP"]
CATEGORIES = ["invoice", "receipt", "contract", "report"]


# path_faces = kagglehub.dataset_download("vishesh1412/celebrity-face-image-dataset") # in case we want more faces, for now HUGE OVERHEAD
path_transactions = kagglehub.dataset_download(
    "valakhorasani/bank-transaction-dataset-for-fraud-detection"
)
df = pd.read_csv(path_transactions + "/bank_transactions_data_2.csv")
user_ids = df["AccountID"].unique()


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


def simulate_transaction(user_id):
    user_transactions = df[df["AccountID"] == user_id]
    if user_transactions.empty:
        print(f"⚠️ No transactions found for {user_id}")
        return

    transaction_row = user_transactions.sample(n=1).iloc[0]

    transaction = {
        "event": "user_transaction",
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": transaction_row["TransactionDate"],
        "type": transaction_row["TransactionType"],
        "amount": float(transaction_row["TransactionAmount"]),
        "currency": random.choice(CURRENCIES),
        "description": f"{transaction_row['MerchantID']} - {transaction_row['Channel']}",
        "location": transaction_row["Location"],
        "device_id": transaction_row["DeviceID"],
        "ip_address": transaction_row["IP Address"],
        "account_balance": float(transaction_row["AccountBalance"]),
        "previous_transaction": transaction_row["PreviousTransactionDate"],
        "transaction_duration": int(transaction_row["TransactionDuration"]),
        "login_attempts": int(transaction_row["LoginAttempts"]),
    }

    try:
        res = requests.post(
            "http://app_backend:6969/submit_transaction/", json=transaction
        )
        print("💸 Transaction Response:", res.json())
    except Exception as e:
        print(f"❌ Failed to send transaction: {e}")


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


def user_exists(user_id):
    try:
        res = requests.get(
            "http://app_backend:6969/user_exists/", params={"user_id": user_id}
        )
        return res.json().get("exists", False)
    except Exception as e:
        print(f"❌ Failed to check if user exists: {e}")
        return False


def register_user(user_id):
    user_row = df[df["AccountID"] == user_id].iloc[0]
    payload = {
        "user_id": user_id,
        "age": int(user_row["CustomerAge"]),
        "occupation": user_row["CustomerOccupation"],
    }

    try:
        res = requests.post("http://app_backend:6969/register_user/", json=payload)
        print("Registered user:", res.json())
    except Exception as e:
        print(f"❌ Failed to register user {user_id}: {e}")


if __name__ == "__main__":
    simulate = {
        1: simulate_kyc,
        2: simulate_pdf_upload,
        3: simulate_transaction,
        4: simulate_transaction,
        5: simulate_transaction,
    }

    while True:
        user_id = random.choice(user_ids)
        if not user_exists(user_id):
            register_user(user_id)
        simulate[random.choice(list(simulate.keys()))](user_id)
        if BIG_DATA:
            # letsss goo: minimal delay
            time.sleep(random.uniform(0, 0.5))
        else:
            time.sleep(random.randint(10, 30))
