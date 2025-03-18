import json
import requests
from util.delta_storage import DeltaStorageHandler

API_URL = "https://api.finnhub.io/api/v1"
DEFAULT_TIMEOUT = 10
API_KEY = "cv6o309r01qi7f6r8o40cv6o309r01qi7f6r8o4g"

def get_financials_reported(symbol=None, cik=None, accessNumber=None, freq="annual", from_date=None, to_date=None):
    params = {
        "symbol": symbol,
        "cik": cik,
        "accessNumber": accessNumber,
        "freq": freq,
        "from": from_date,
        "to": to_date,
        "token": API_KEY
    }
    params = {k: v for k, v in params.items() if v is not None}  # Remove None values

    response = requests.get(f"{API_URL}/stock/financials-reported", params=params, timeout=DEFAULT_TIMEOUT)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed: {response.status_code}, {response.text}")

# Initialize DeltaStorageHandler (this will create the Spark session)
storage = DeltaStorageHandler()

# Example usage:
financials_report = get_financials_reported(symbol="AAPL", freq="quarterly", from_date="2023-01-01", to_date="2023-12-31")
print(financials_report)

if financials_report:
    # Write the ECB data to a Delta table
    storage.write_api_json(financials_report, "financials_reported_finnhub", mode="overwrite")
    print("Finnhub Financial Report data written to Delta Lake.")
else:
    print("Failed to fetch Finnhub Financial Report data.")

# Stop the Spark session when done
storage.stop_spark()